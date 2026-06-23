package fr.assurance.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
public class BenchmarkController {

    private final BenchmarkService service;
    private final ObjectMapper mapper;

    private static final AtomicInteger THREAD_COUNT = new AtomicInteger();
    private static final ThreadFactory DAEMON_FACTORY = r -> {
        Thread t = new Thread(r, "benchmark-" + THREAD_COUNT.incrementAndGet());
        t.setDaemon(true);
        return t;
    };

    // Pool partagé (single-thread) : le mutex dans BenchmarkService empêche les doublons.
    private final ExecutorService executor = Executors.newSingleThreadExecutor(DAEMON_FACTORY);

    public BenchmarkController(BenchmarkService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper  = mapper;
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    @GetMapping("/")
    public String index() { return "index"; }

    @GetMapping("/benchmark/health")
    @ResponseBody
    public Map<String, String> health() { return service.checkHealth(); }

    @GetMapping("/benchmark/durability")
    @ResponseBody
    public Map<String, String> durability() { return service.durabilityInfo(); }

    @GetMapping("/benchmark")
    @ResponseBody
    public BenchmarkResult benchmarkJson(
            @RequestParam(defaultValue = "500")  int warmup,
            @RequestParam(defaultValue = "5000") int messages
    ) throws Exception {
        return service.run(warmup, messages);
    }

    @GetMapping(value = "/benchmark/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @ResponseBody
    public SseEmitter benchmarkStream(
            @RequestParam(defaultValue = "200")  int warmup,
            @RequestParam(defaultValue = "2000") int messages,
            @RequestParam(defaultValue = "0")    int payloadSize,
            @RequestParam(defaultValue = "true") boolean artemis,
            @RequestParam(defaultValue = "true") boolean pulsar,
            @RequestParam(defaultValue = "1")    int producerCount,
            @RequestParam(defaultValue = "1")    int runs
    ) {
        BenchmarkParams params = new BenchmarkParams(
                warmup, messages, payloadSize, artemis, pulsar,
                Math.max(1, Math.min(producerCount, 8)),
                Math.max(1, Math.min(runs, 5)));
        SseEmitter emitter = new SseEmitter(600_000L);

        executor.submit(() -> {
            try {
                validateBenchParams(messages, payloadSize, producerCount, runs);
                service.runStreaming(params, progress -> {
                    try { emitter.send(SseEmitter.event().data(mapper.writeValueAsString(progress))); }
                    catch (Exception e) { emitter.completeWithError(e); }
                });
                emitter.complete();
            } catch (IllegalArgumentException | IllegalStateException e) {
                try { emitter.send(SseEmitter.event().name("bench-error").data(e.getMessage())); } catch (Exception ignored) {}
                emitter.complete();
            } catch (Exception e) { emitter.completeWithError(e); }
        });

        return emitter;
    }

    private static void validateBenchParams(int messages, int payloadSize, int producerCount, int runs) {
        if (messages < 100 || messages > 50_000)
            throw new IllegalArgumentException("Messages hors borne : attendu 100–50 000, reçu " + messages);
        if (payloadSize < 0 || payloadSize > 65_536)
            throw new IllegalArgumentException("Payload hors borne : attendu 0–65 536 B, reçu " + payloadSize);
        if (producerCount < 1 || producerCount > 8)
            throw new IllegalArgumentException("Producteurs hors borne : attendu 1–8, reçu " + producerCount);
        if (runs < 1 || runs > 5)
            throw new IllegalArgumentException("Runs hors borne : attendu 1–5, reçu " + runs);
    }

    @GetMapping(value = "/benchmark/sweep/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @ResponseBody
    public SseEmitter sweepStream(
            @RequestParam(defaultValue = "50")   int warmup,
            @RequestParam(defaultValue = "500")  int messages,
            @RequestParam(defaultValue = "true") boolean artemis,
            @RequestParam(defaultValue = "true") boolean pulsar
    ) {
        BenchmarkParams params = new BenchmarkParams(warmup, messages, 0, artemis, pulsar, 1, 1);
        SseEmitter emitter = new SseEmitter(600_000L);

        executor.submit(() -> {
            try {
                service.sweepStreaming(params, point -> {
                    try { emitter.send(SseEmitter.event().data(mapper.writeValueAsString(point))); }
                    catch (Exception e) { emitter.completeWithError(e); }
                });
                emitter.complete();
            } catch (IllegalStateException e) {
                try { emitter.send(SseEmitter.event().name("bench-error").data(e.getMessage())); } catch (Exception ignored) {}
                emitter.complete();
            } catch (Exception e) { emitter.completeWithError(e); }
        });

        return emitter;
    }
}
