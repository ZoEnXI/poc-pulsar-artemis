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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Service
public class BenchmarkService {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkService.class);
    private static final int REPORT_INTERVAL = 100;

    private final EmbeddedArtemisServer artemisServer;
    private final EmbeddedPulsarServer  pulsarServer;
    private final ObjectMapper mapper;

    public BenchmarkService(EmbeddedArtemisServer artemisServer, EmbeddedPulsarServer pulsarServer) {
        this.artemisServer = artemisServer;
        this.pulsarServer  = pulsarServer;
        this.mapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .findAndRegisterModules();
    }

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
        byte[] payload = buildPayload(params.payloadSize());
        log.info("Stream — payload={}B warmup={} messages={} producers={} artemis={} pulsar={}",
                payload.length, params.warmup(), params.messages(),
                params.producerCount(), params.artemis(), params.pulsar());

        if (params.artemis()) {
            if (params.producerCount() > 1) streamArtemisPar(payload, params, cb);
            else                            streamArtemis(payload, params, cb);
        }
        if (params.pulsar()) {
            if (params.producerCount() > 1) streamPulsarPar(payload, params, cb);
            else                            streamPulsar(payload, params, cb);
        }
    }

    // ── Artemis — single producer, E2E mesuré ─────────────────────────────────

    private BenchmarkResult.BrokerMetrics benchmarkArtemis(byte[] payload, int warmup, int messages) throws Exception {
        try (ArtemisBenchmarkClient c = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl())) {
            warmupArtemis(c, payload, warmup);
            long[][] lats = measureArtemis(c, payload, messages);
            return toMetrics(messages, lats[0], lats[1], wallTime(lats[0]));
        }
    }

    private void streamArtemis(byte[] payload, BenchmarkParams p, Consumer<BenchmarkProgress> cb) throws Exception {
        int n = p.messages();
        try (ArtemisBenchmarkClient c = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl())) {
            warmupArtemis(c, payload, p.warmup());
            long[] pub = new long[n], e2e = new long[n];
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                long[] both = c.sendAndMeasureBoth(payload);
                pub[i] = both[0]; e2e[i] = both[1];
                if ((i + 1) % REPORT_INTERVAL == 0)
                    cb.accept(partial("artemis", i + 1, n, pub, e2e, System.nanoTime() - t0));
            }
            cb.accept(finalEvt("artemis", n, pub, e2e, System.nanoTime() - t0));
        }
    }

    private static void warmupArtemis(ArtemisBenchmarkClient c, byte[] payload, int warmup) throws Exception {
        for (int i = 0; i < warmup; i++) c.sendAndMeasure(payload);
        c.drain(warmup, 10_000);
    }

    private static long[][] measureArtemis(ArtemisBenchmarkClient c, byte[] payload, int n) throws Exception {
        long[] pub = new long[n], e2e = new long[n];
        for (int i = 0; i < n; i++) {
            long[] both = c.sendAndMeasureBoth(payload);
            pub[i] = both[0]; e2e[i] = both[1];
        }
        return new long[][]{pub, e2e};
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
            cb.accept(parProgress("artemis", sent.get(), actual, System.nanoTime() - t0));
        pool.shutdown();
        if (err.get() != null) throw err.get();

        long elapsed = System.nanoTime() - t0;
        try (ArtemisBenchmarkClient dc = new ArtemisBenchmarkClient(artemisServer.getBrokerUrl())) { dc.drain(actual, 60_000); }

        long[] all = merge(tlat, threads, perThread);
        cb.accept(finalEvt("artemis", actual, all, EMPTY, elapsed));
    }

    // ── Pulsar — single producer, E2E mesuré ──────────────────────────────────

    private BenchmarkResult.BrokerMetrics benchmarkPulsar(byte[] payload, int warmup, int messages) throws Exception {
        try (PulsarBenchmarkClient c = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl())) {
            warmupPulsar(c, payload, warmup);
            long[][] lats = measurePulsar(c, payload, messages);
            return toMetrics(messages, lats[0], lats[1], wallTime(lats[0]));
        }
    }

    private void streamPulsar(byte[] payload, BenchmarkParams p, Consumer<BenchmarkProgress> cb) throws Exception {
        int n = p.messages();
        try (PulsarBenchmarkClient c = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl())) {
            warmupPulsar(c, payload, p.warmup());
            long[] pub = new long[n], e2e = new long[n];
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                long[] both = c.sendAndMeasureBoth(payload, "k" + i);
                pub[i] = both[0]; e2e[i] = both[1];
                if ((i + 1) % REPORT_INTERVAL == 0)
                    cb.accept(partial("pulsar", i + 1, n, pub, e2e, System.nanoTime() - t0));
            }
            cb.accept(finalEvt("pulsar", n, pub, e2e, System.nanoTime() - t0));
        }
    }

    private static void warmupPulsar(PulsarBenchmarkClient c, byte[] payload, int warmup) throws Exception {
        for (int i = 0; i < warmup; i++) c.sendAndMeasure(payload, "warmup");
        c.drain(warmup, 10_000);
    }

    private static long[][] measurePulsar(PulsarBenchmarkClient c, byte[] payload, int n) throws Exception {
        long[] pub = new long[n], e2e = new long[n];
        for (int i = 0; i < n; i++) {
            long[] both = c.sendAndMeasureBoth(payload, "k" + i);
            pub[i] = both[0]; e2e[i] = both[1];
        }
        return new long[][]{pub, e2e};
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
            cb.accept(parProgress("pulsar", sent.get(), actual, System.nanoTime() - t0));
        pool.shutdown();
        if (err.get() != null) throw err.get();

        long elapsed = System.nanoTime() - t0;
        try (PulsarBenchmarkClient dc = new PulsarBenchmarkClient(pulsarServer.getBrokerUrl())) { dc.drain(actual, 60_000); }

        long[] all = merge(tlat, threads, perThread);
        cb.accept(finalEvt("pulsar", actual, all, EMPTY, elapsed));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final long[] EMPTY = new long[0];

    private static BenchmarkProgress partial(String broker, int sent, int total,
                                              long[] pub, long[] e2e, long elapsedNs) {
        long[] sp = Arrays.copyOf(pub, sent); Arrays.sort(sp);
        long[] se = Arrays.copyOf(e2e, sent); Arrays.sort(se);
        return new BenchmarkProgress(broker, false, sent, total,
                toMs(sp[(int)(sent * 0.50)]),
                toMs(sp[(int)(sent * 0.99)]),
                toMs(sp[clamp(sent, 0.999)]),
                sent / (elapsedNs / 1e9),
                toMs(se[(int)(sent * 0.50)]),
                toMs(se[(int)(sent * 0.99)]),
                toMs(se[clamp(sent, 0.999)]));
    }

    private static BenchmarkProgress finalEvt(String broker, int n,
                                               long[] pub, long[] e2e, long elapsedNs) {
        long[] sp = pub.clone(); Arrays.sort(sp);
        boolean hasE2e = e2e.length == n;
        long[] se = hasE2e ? e2e.clone() : new long[n];
        if (hasE2e) Arrays.sort(se);
        return new BenchmarkProgress(broker, true, n, n,
                toMs(sp[(int)(n * 0.50)]),
                toMs(sp[(int)(n * 0.99)]),
                toMs(sp[clamp(n, 0.999)]),
                n / (elapsedNs / 1e9),
                hasE2e ? toMs(se[(int)(n * 0.50)])      : 0,
                hasE2e ? toMs(se[(int)(n * 0.99)])      : 0,
                hasE2e ? toMs(se[clamp(n, 0.999)])      : 0);
    }

    private static BenchmarkProgress parProgress(String broker, int sent, int total, long elapsedNs) {
        return new BenchmarkProgress(broker, false, sent, total,
                0, 0, 0, sent / (elapsedNs / 1e9), 0, 0, 0);
    }

    private static BenchmarkResult.BrokerMetrics toMetrics(int n, long[] pub, long[] e2e, long elapsedNs) {
        long[] sp = pub.clone(); Arrays.sort(sp);
        long[] se = e2e.clone(); Arrays.sort(se);
        return new BenchmarkResult.BrokerMetrics(n,
                toMs(sp[(int)(n * 0.50)]),
                toMs(sp[(int)(n * 0.99)]),
                toMs(sp[clamp(n, 0.999)]),
                n / (elapsedNs / 1e9),
                toMs(se[(int)(n * 0.50)]),
                toMs(se[(int)(n * 0.99)]),
                toMs(se[clamp(n, 0.999)]));
    }

    private static long[] merge(long[][] t, int threads, int perThread) {
        long[] all = new long[threads * perThread];
        for (int i = 0; i < threads; i++) System.arraycopy(t[i], 0, all, i * perThread, perThread);
        return all;
    }

    /** Temps total basé sur la somme des latences (approximation wall-time pour mode séquentiel). */
    private static long wallTime(long[] lat) {
        long sum = 0; for (long l : lat) sum += l; return sum;
    }

    private static int clamp(int n, double pct) { return Math.max(0, (int)(n * pct) - 1); }
    private static double toMs(long nanos) { return nanos / 1_000_000.0; }

    private byte[] buildPayload(int size) throws Exception {
        if (size > 0) { byte[] b = new byte[size]; new Random().nextBytes(b); return b; }
        return mapper.writeValueAsBytes(sampleEvent());
    }

    private static ContratEvent sampleEvent() {
        return new ContratEvent(UUID.randomUUID().toString(), EventType.SOUSCRIPTION,
                Instant.now(), "Dupont Jean", "PREVOYANCE-VIE", 100_000.00);
    }
}
