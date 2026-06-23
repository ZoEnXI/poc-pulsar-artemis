package fr.assurance.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.assurance.artemis.ArtemisBenchmarkClient;
import fr.assurance.artemis.EmbeddedArtemisServer;
import fr.assurance.pulsar.EmbeddedPulsarServer;
import fr.assurance.pulsar.PulsarBenchmarkClient;
import fr.assurance.runner.domain.ContratEvent;
import fr.assurance.runner.domain.ContratEvent.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


@Service
public class BenchmarkService {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkService.class);
    private static final int REPORT_INTERVAL = 100;

    private static final int[]    SWEEP_SIZES  = {128, 1024, 10240, 65536};
    private static final String[] SWEEP_LABELS = {"128 B", "1 KB", "10 KB", "64 KB"};

    private final EmbeddedArtemisServer artemisServer;
    private final EmbeddedPulsarServer  pulsarServer;
    private final ObjectMapper mapper;

    // Mutex : un seul benchmark (streaming ou sweep) à la fois
    private final AtomicBoolean benchmarkRunning = new AtomicBoolean(false);

    public BenchmarkService(EmbeddedArtemisServer artemisServer, EmbeddedPulsarServer pulsarServer) {
        this.artemisServer = artemisServer;
        this.pulsarServer  = pulsarServer;
        this.mapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .findAndRegisterModules();
    }

    public boolean isRunning() { return benchmarkRunning.get(); }

    // ── Health check ──────────────────────────────────────────────────────────

    public Map<String, String> checkHealth() {
        Map<String, String> res = new LinkedHashMap<>();
        res.put("artemis", probeArtemis());
        res.put("pulsar",  probePulsar());
        return res;
    }

    private String probeArtemis() {
        try (ArtemisBenchmarkClient c = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl())) {
            c.sendAndMeasureBoth(new byte[]{1});
            return "READY";
        } catch (Exception e) { return "ERROR: " + e.getMessage(); }
    }

    private String probePulsar() {
        try (PulsarBenchmarkClient c = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl())) {
            c.sendAndMeasureBoth(new byte[]{1}, "health");
            return "READY";
        } catch (Exception e) { return "ERROR: " + e.getMessage(); }
    }

    // ── Durability info ───────────────────────────────────────────────────────

    public Map<String, String> durabilityInfo() {
        Map<String, String> info = new LinkedHashMap<>();
        info.put("artemis", artemisServer.isPersistent() ? "journal (tmpdir, no fsync)" : "in-memory");
        info.put("pulsar",  "BookKeeper (tmpdir, no fsync)");
        return info;
    }

    // ── Legacy synchronous run ────────────────────────────────────────────────

    public BenchmarkResult run(int warmup, int messages) throws Exception {
        byte[] payload = mapper.writeValueAsBytes(sampleEvent());
        log.info("Run — payload={}B warmup={} messages={}", payload.length, warmup, messages);
        return new BenchmarkResult(
                benchmarkArtemis(payload, warmup, messages),
                benchmarkPulsar(payload, warmup, messages));
    }

    // ── Streaming entry point ─────────────────────────────────────────────────

    public void runStreaming(BenchmarkParams params, Consumer<BenchmarkProgress> cb) throws Exception {
        if (!benchmarkRunning.compareAndSet(false, true))
            throw new IllegalStateException("Un benchmark est déjà en cours — réessayez dans quelques instants.");
        try {
            byte[] payload = buildPayload(params.payloadSize());
            log.info("Stream — payload={}B warmup={} messages={} producers={} runs={} artemis={} pulsar={}",
                    payload.length, params.warmup(), params.messages(),
                    params.producerCount(), params.runs(), params.artemis(), params.pulsar());

            if (params.runs() > 1) {
                runStreamingMulti(payload, params, cb);
            } else {
                runStreamingSingle(payload, params, cb);
            }
        } finally {
            benchmarkRunning.set(false);
        }
    }

    // ── Single run — streaming par message ───────────────────────────────────

    private void runStreamingSingle(byte[] payload, BenchmarkParams p, Consumer<BenchmarkProgress> cb) throws Exception {
        if (p.artemis()) {
            if (p.producerCount() > 1) streamArtemisPar(payload, p, cb);
            else                       streamArtemis(payload, p, cb);
        }
        if (p.pulsar()) {
            if (p.producerCount() > 1) streamPulsarPar(payload, p, cb);
            else                       streamPulsar(payload, p, cb);
        }
    }

    // ── Multi-run — silent, émet un résumé par run + stddev finale ───────────

    private void runStreamingMulti(byte[] payload, BenchmarkParams p, Consumer<BenchmarkProgress> cb) throws Exception {
        int N = p.runs();
        BenchmarkResult.BrokerMetrics[] artRuns = new BenchmarkResult.BrokerMetrics[N];
        BenchmarkResult.BrokerMetrics[] pulRuns = new BenchmarkResult.BrokerMetrics[N];

        for (int r = 0; r < N; r++) {
            int run = r + 1;
            log.info("Multi-run {}/{}", run, N);
            if (p.artemis()) {
                artRuns[r] = benchmarkArtemis(payload, p.warmup(), p.messages());
                cb.accept(runSummaryEvt("artemis", run, N, artRuns[r]));
            }
            if (p.pulsar()) {
                pulRuns[r] = benchmarkPulsar(payload, p.warmup(), p.messages());
                cb.accept(runSummaryEvt("pulsar", run, N, pulRuns[r]));
            }
        }

        if (p.artemis()) cb.accept(stddevEvt("artemis", N, artRuns));
        if (p.pulsar())  cb.accept(stddevEvt("pulsar",  N, pulRuns));
    }

    private static BenchmarkProgress runSummaryEvt(String broker, int run, int totalRuns,
                                                    BenchmarkResult.BrokerMetrics m) {
        return new BenchmarkProgress(broker, false,
                m.messagesSent(), m.messagesSent(),
                m.p50Ms(), m.p99Ms(), m.p999Ms(), m.throughputMsgSec(),
                m.e2eP50Ms(), m.e2eP99Ms(), m.e2eP999Ms(),
                run, totalRuns, 0, 0, false);
    }

    private static BenchmarkProgress stddevEvt(String broker, int runs,
                                                BenchmarkResult.BrokerMetrics[] ms) {
        double p50  = mean(ms, BenchmarkResult.BrokerMetrics::p50Ms);
        double p99  = mean(ms, BenchmarkResult.BrokerMetrics::p99Ms);
        double p999 = mean(ms, BenchmarkResult.BrokerMetrics::p999Ms);
        double tp   = mean(ms, BenchmarkResult.BrokerMetrics::throughputMsgSec);
        double e50  = mean(ms, BenchmarkResult.BrokerMetrics::e2eP50Ms);
        double e99  = mean(ms, BenchmarkResult.BrokerMetrics::e2eP99Ms);
        double e999 = mean(ms, BenchmarkResult.BrokerMetrics::e2eP999Ms);
        double stdP99  = stddev(ms, BenchmarkResult.BrokerMetrics::p99Ms,  p99);
        double stdE99  = stddev(ms, BenchmarkResult.BrokerMetrics::e2eP99Ms, e99);
        return new BenchmarkProgress(broker, true,
                ms[0].messagesSent(), ms[0].messagesSent(),
                p50, p99, p999, tp, e50, e99, e999,
                runs, runs, stdP99, stdE99, true);
    }

    // ── Sweep — p99 et débit vs taille payload ────────────────────────────────

    public void sweepStreaming(BenchmarkParams params, Consumer<SweepProgress> cb) throws Exception {
        if (!benchmarkRunning.compareAndSet(false, true))
            throw new IllegalStateException("Un benchmark est déjà en cours — réessayez dans quelques instants.");
        try {
            log.info("Sweep — warmup={} messages={} artemis={} pulsar={}",
                    params.warmup(), params.messages(), params.artemis(), params.pulsar());

            Random rng = new Random();
            List<SweepPoint> points = new ArrayList<>();

            for (int si = 0; si < SWEEP_SIZES.length; si++) {
                int size = SWEEP_SIZES[si];
                String label = SWEEP_LABELS[si];
                byte[] payload = new byte[size];
                rng.nextBytes(payload);

                cb.accept(new SweepProgress("MEASURING", si, SWEEP_SIZES.length, size, label,
                        0, 0, 0, 0, List.copyOf(points)));

                BenchmarkResult.BrokerMetrics artResult = null, pulResult = null;
                if (params.artemis()) artResult = benchmarkArtemis(payload, params.warmup(), params.messages());
                if (params.pulsar())  pulResult = benchmarkPulsar(payload,  params.warmup(), params.messages());

                double artP99   = artResult != null ? artResult.p99Ms() : 0;
                double pulP99   = pulResult != null ? pulResult.p99Ms() : 0;
                double artMbSec = artResult != null ? artResult.throughputMsgSec() * size / (1024.0 * 1024.0) : 0;
                double pulMbSec = pulResult != null ? pulResult.throughputMsgSec() * size / (1024.0 * 1024.0) : 0;

                points.add(new SweepPoint(size, label, artP99, pulP99, artMbSec, pulMbSec));
                cb.accept(new SweepProgress("POINT", si, SWEEP_SIZES.length, size, label,
                        artP99, pulP99, artMbSec, pulMbSec, List.copyOf(points)));
            }

            cb.accept(new SweepProgress("DONE", SWEEP_SIZES.length - 1, SWEEP_SIZES.length, 0, "",
                    0, 0, 0, 0, List.copyOf(points)));
        } finally {
            benchmarkRunning.set(false);
        }
    }

    // ── Artemis — single producer, consumer concurrent ────────────────────────
    //
    // Même architecture que Pulsar : le consumer tourne sur un thread daemon
    // pendant que le producteur envoie tous les messages sans attendre.
    // Artemis garantit l'ordre FIFO sur une queue single-producer, donc
    // recvNs[i] correspond à sendNs[i] par position (pas besoin de seqno dans le message).

    private BenchmarkResult.BrokerMetrics benchmarkArtemis(byte[] payload, int warmup, int messages) throws Exception {
        try (ArtemisBenchmarkClient c = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl())) {
            warmupArtemis(c, payload, warmup);
            int n = messages;
            long[] pub    = new long[n];
            long[] sendNs = new long[n];

            Future<long[]> recvFuture = c.consumeAsync(n);

            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                long[] sr = c.sendAndRecord(payload);
                sendNs[i] = sr[0];
                pub[i]    = sr[1];
            }
            long sendElapsed = System.nanoTime() - t0;

            long[] recvNs = recvFuture.get(60, TimeUnit.SECONDS);
            long[] e2e    = computeE2e(pub, sendNs, recvNs, n);
            return toMetrics(n, pub, e2e, sendElapsed);
        }
    }

    private void streamArtemis(byte[] payload, BenchmarkParams p, Consumer<BenchmarkProgress> cb) throws Exception {
        int n = p.messages();
        try (ArtemisBenchmarkClient c = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl())) {
            warmupArtemis(c, payload, p.warmup());
            long[] pub    = new long[n];
            long[] sendNs = new long[n];

            Future<long[]> recvFuture = c.consumeAsync(n);

            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                long[] sr = c.sendAndRecord(payload);
                sendNs[i] = sr[0];
                pub[i]    = sr[1];
                if ((i + 1) % REPORT_INTERVAL == 0)
                    cb.accept(partial("artemis", i + 1, n, pub, new long[0], System.nanoTime() - t0, 1, 1));
            }
            long sendElapsed = System.nanoTime() - t0;

            long[] recvNs = recvFuture.get(60, TimeUnit.SECONDS);
            long[] e2e    = computeE2e(pub, sendNs, recvNs, n);
            cb.accept(finalEvt("artemis", n, pub, e2e, sendElapsed, 1, 1));
        }
    }

    private static void warmupArtemis(ArtemisBenchmarkClient c, byte[] payload, int warmup) throws Exception {
        for (int i = 0; i < warmup; i++) c.sendAndMeasure(payload);
        c.drain(warmup, 10_000);
    }

    // ── Artemis — parallel producers (pub latency only) ───────────────────────

    private void streamArtemisPar(byte[] payload, BenchmarkParams p, Consumer<BenchmarkProgress> cb) throws Exception {
        int threads = p.producerCount(), perThread = p.messages() / threads, actual = perThread * threads;
        log.info("Artemis par: {} × {} = {} msgs", threads, perThread, actual);

        try (ArtemisBenchmarkClient wc = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl())) {
            warmupArtemis(wc, payload, p.warmup());
        }

        AtomicInteger sent = new AtomicInteger();
        AtomicReference<Exception> err = new AtomicReference<>();
        long[][] tlat = new long[threads][perThread];
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);

        long t0 = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try (ArtemisBenchmarkClient c = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl(), true)) {
                    for (int i = 0; i < perThread; i++) { tlat[tid][i] = c.sendAndMeasure(payload); sent.incrementAndGet(); }
                } catch (Exception e) { err.compareAndSet(null, e); } finally { done.countDown(); }
            });
        }
        while (!done.await(300, TimeUnit.MILLISECONDS))
            cb.accept(parProgress("artemis", sent.get(), actual, System.nanoTime() - t0, 1, 1));
        pool.shutdown();
        if (err.get() != null) throw err.get();

        long elapsed = System.nanoTime() - t0;
        try (ArtemisBenchmarkClient dc = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl())) { dc.drain(actual, 60_000); }

        long[] all = merge(tlat, threads, perThread);
        cb.accept(finalEvt("artemis", actual, all, EMPTY, elapsed, 1, 1));
    }

    // ── Pulsar — single producer, consumer concurrent ─────────────────────────
    //
    // Le consumer tourne sur un thread daemon pendant que le producteur envoie
    // tous les messages en séquence. L'e2e est calculé message par message via
    // les timestamps sendNs[i] et recvNs[i] corrélés par le seqno (= la key).

    private BenchmarkResult.BrokerMetrics benchmarkPulsar(byte[] payload, int warmup, int messages) throws Exception {
        try (PulsarBenchmarkClient c = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl())) {
            warmupPulsar(c, payload, warmup);
            int n = messages;
            long[] pub    = new long[n];
            long[] sendNs = new long[n];

            Future<long[]> recvFuture = c.consumeAsync(n);

            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                long[] sr = c.sendAndRecord(payload, String.valueOf(i));
                sendNs[i] = sr[0];
                pub[i]    = sr[1];
            }
            long sendElapsed = System.nanoTime() - t0;

            long[] recvNs = recvFuture.get(60, TimeUnit.SECONDS);
            long[] e2e    = computeE2e(pub, sendNs, recvNs, n);
            return toMetrics(n, pub, e2e, sendElapsed);
        }
    }

    private void streamPulsar(byte[] payload, BenchmarkParams p, Consumer<BenchmarkProgress> cb) throws Exception {
        int n = p.messages();
        try (PulsarBenchmarkClient c = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl())) {
            warmupPulsar(c, payload, p.warmup());
            long[] pub    = new long[n];
            long[] sendNs = new long[n];

            Future<long[]> recvFuture = c.consumeAsync(n);

            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                long[] sr = c.sendAndRecord(payload, String.valueOf(i));
                sendNs[i] = sr[0];
                pub[i]    = sr[1];
                if ((i + 1) % REPORT_INTERVAL == 0)
                    cb.accept(partial("pulsar", i + 1, n, pub, new long[0], System.nanoTime() - t0, 1, 1));
            }
            long sendElapsed = System.nanoTime() - t0;

            long[] recvNs = recvFuture.get(60, TimeUnit.SECONDS);
            long[] e2e    = computeE2e(pub, sendNs, recvNs, n);
            cb.accept(finalEvt("pulsar", n, pub, e2e, sendElapsed, 1, 1));
        }
    }

    private static void warmupPulsar(PulsarBenchmarkClient c, byte[] payload, int warmup) throws Exception {
        for (int i = 0; i < warmup; i++) c.sendAndMeasure(payload, "warmup");
        c.drain(warmup, 10_000);
    }

    /**
     * Calcule la latence E2E par message.
     * Pour Pulsar : corrélation par seqno (recvNs[i] = reception du message i).
     * Pour Artemis : corrélation par position FIFO (recvNs[i] = reception du i-ème message en ordre).
     * Si recvNs[i] == 0 (message non reçu dans le timeout), on utilise pub[i] comme fallback.
     */
    private static long[] computeE2e(long[] pub, long[] sendNs, long[] recvNs, int n) {
        long[] e2e = new long[n];
        for (int i = 0; i < n; i++) {
            if (recvNs[i] > 0) {
                long raw = recvNs[i] - sendNs[i];
                if (raw < pub[i]) {
                    log.warn("E2E[{}] < pubLat ({} < {} ns) — incohérence timestamp, fallback sur pubLat", i, raw, pub[i]);
                    e2e[i] = pub[i];
                } else {
                    e2e[i] = raw;
                }
            } else {
                e2e[i] = pub[i]; // message non reçu dans le timeout
            }
        }
        return e2e;
    }

    // ── Pulsar — parallel producers (pub latency only) ────────────────────────

    private void streamPulsarPar(byte[] payload, BenchmarkParams p, Consumer<BenchmarkProgress> cb) throws Exception {
        int threads = p.producerCount(), perThread = p.messages() / threads, actual = perThread * threads;
        log.info("Pulsar par: {} × {} = {} msgs", threads, perThread, actual);

        try (PulsarBenchmarkClient wc = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl())) {
            warmupPulsar(wc, payload, p.warmup());
        }

        AtomicInteger sent = new AtomicInteger();
        AtomicReference<Exception> err = new AtomicReference<>();
        long[][] tlat = new long[threads][perThread];
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);

        long t0 = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try (PulsarBenchmarkClient c = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl(), true)) {
                    for (int i = 0; i < perThread; i++) { tlat[tid][i] = c.sendAndMeasure(payload, "t" + tid + "k" + i); sent.incrementAndGet(); }
                } catch (Exception e) { err.compareAndSet(null, e); } finally { done.countDown(); }
            });
        }
        while (!done.await(300, TimeUnit.MILLISECONDS))
            cb.accept(parProgress("pulsar", sent.get(), actual, System.nanoTime() - t0, 1, 1));
        pool.shutdown();
        if (err.get() != null) throw err.get();

        long elapsed = System.nanoTime() - t0;
        try (PulsarBenchmarkClient dc = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl())) { dc.drain(actual, 60_000); }

        long[] all = merge(tlat, threads, perThread);
        cb.accept(finalEvt("pulsar", actual, all, EMPTY, elapsed, 1, 1));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final long[] EMPTY = new long[0];

    private static BenchmarkProgress partial(String broker, int sent, int total,
                                              long[] pub, long[] e2e, long elapsedNs,
                                              int run, int totalRuns) {
        long[] sp = Arrays.copyOf(pub, sent); Arrays.sort(sp);
        long[] se = Arrays.copyOf(e2e, sent); Arrays.sort(se);
        return new BenchmarkProgress(broker, false, sent, total,
                toMs(sp[clamp(sent, 0.50)]),
                toMs(sp[clamp(sent, 0.99)]),
                toMs(sp[clamp(sent, 0.999)]),
                sent / (elapsedNs / 1e9),
                toMs(se[clamp(sent, 0.50)]),
                toMs(se[clamp(sent, 0.99)]),
                toMs(se[clamp(sent, 0.999)]),
                run, totalRuns, 0, 0, false);
    }

    private static BenchmarkProgress finalEvt(String broker, int n,
                                               long[] pub, long[] e2e, long elapsedNs,
                                               int run, int totalRuns) {
        long[] sp = pub.clone(); Arrays.sort(sp);
        boolean hasE2e = e2e.length == n;
        long[] se = hasE2e ? e2e.clone() : new long[n];
        if (hasE2e) Arrays.sort(se);
        return new BenchmarkProgress(broker, true, n, n,
                toMs(sp[clamp(n, 0.50)]),
                toMs(sp[clamp(n, 0.99)]),
                toMs(sp[clamp(n, 0.999)]),
                n / (elapsedNs / 1e9),
                hasE2e ? toMs(se[clamp(n, 0.50)])  : 0,
                hasE2e ? toMs(se[clamp(n, 0.99)])  : 0,
                hasE2e ? toMs(se[clamp(n, 0.999)]) : 0,
                run, totalRuns, 0, 0, true);
    }

    private static BenchmarkProgress parProgress(String broker, int sent, int total,
                                                   long elapsedNs, int run, int totalRuns) {
        return new BenchmarkProgress(broker, false, sent, total,
                0, 0, 0, sent / (elapsedNs / 1e9), 0, 0, 0,
                run, totalRuns, 0, 0, false);
    }

    private static BenchmarkResult.BrokerMetrics toMetrics(int n, long[] pub, long[] e2e, long elapsedNs) {
        long[] sp = pub.clone(); Arrays.sort(sp);
        long[] se = e2e.clone(); Arrays.sort(se);
        return new BenchmarkResult.BrokerMetrics(n,
                toMs(sp[clamp(n, 0.50)]),
                toMs(sp[clamp(n, 0.99)]),
                toMs(sp[clamp(n, 0.999)]),
                n / (elapsedNs / 1e9),
                toMs(se[clamp(n, 0.50)]),
                toMs(se[clamp(n, 0.99)]),
                toMs(se[clamp(n, 0.999)]));
    }

    private static long[] merge(long[][] t, int threads, int perThread) {
        long[] all = new long[threads * perThread];
        for (int i = 0; i < threads; i++) System.arraycopy(t[i], 0, all, i * perThread, perThread);
        return all;
    }

    /**
     * Formule correcte : le p-ème percentile est la plus petite valeur telle que
     * p% des données sont inférieures ou égales à elle.
     * Index = ceil(n * pct) - 1, clampé à [0, n-1].
     * Corrige le bug précédent où (int)(n * pct) sur-estimait systématiquement d'un rang,
     * et où p99.9 était confondu avec le max pour n < 10 000.
     */
    private static int clamp(int n, double pct) {
        return Math.min(n - 1, Math.max(0, (int) Math.ceil(n * pct) - 1));
    }

    private static double toMs(long nanos) { return nanos / 1_000_000.0; }

    private byte[] buildPayload(int size) throws Exception {
        if (size > 0) { byte[] b = new byte[size]; new Random().nextBytes(b); return b; }
        return mapper.writeValueAsBytes(sampleEvent());
    }

    private static ContratEvent sampleEvent() {
        return new ContratEvent(UUID.randomUUID().toString(), EventType.SOUSCRIPTION,
                Instant.now(), "Dupont Jean", "PREVOYANCE-VIE", 100_000.00);
    }

    // ── Statistiques inter-runs ───────────────────────────────────────────────

    @FunctionalInterface
    private interface MetricExtractor {
        double get(BenchmarkResult.BrokerMetrics m);
    }

    private static double mean(BenchmarkResult.BrokerMetrics[] ms, MetricExtractor f) {
        double sum = 0;
        for (BenchmarkResult.BrokerMetrics m : ms) sum += f.get(m);
        return sum / ms.length;
    }

    /** Stddev avec correction de Bessel (diviseur N-1) pour estimation non biaisée. */
    private static double stddev(BenchmarkResult.BrokerMetrics[] ms, MetricExtractor f, double mean) {
        if (ms.length < 2) return 0;
        double sum = 0;
        for (BenchmarkResult.BrokerMetrics m : ms) sum += Math.pow(f.get(m) - mean, 2);
        return Math.sqrt(sum / (ms.length - 1));
    }
}
