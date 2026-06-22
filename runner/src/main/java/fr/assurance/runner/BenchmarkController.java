package fr.assurance.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.Executors;

@Controller
public class BenchmarkController {

    private final BenchmarkService service;
    private final ObjectMapper mapper;

    public BenchmarkController(BenchmarkService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper  = mapper;
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
            @RequestParam(defaultValue = "1")    int producerCount
    ) {
        BenchmarkParams params = new BenchmarkParams(
                warmup, messages, payloadSize, artemis, pulsar, Math.max(1, producerCount));
        SseEmitter emitter = new SseEmitter(300_000L);

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                service.runStreaming(params, progress -> {
                    try { emitter.send(SseEmitter.event().data(mapper.writeValueAsString(progress))); }
                    catch (Exception e) { emitter.completeWithError(e); }
                });
                emitter.complete();
            } catch (Exception e) { emitter.completeWithError(e); }
        });

        return emitter;
    }
}
