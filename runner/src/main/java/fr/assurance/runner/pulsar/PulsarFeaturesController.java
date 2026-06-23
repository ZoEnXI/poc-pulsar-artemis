package fr.assurance.runner.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
public class PulsarFeaturesController {

    private final PulsarFeaturesService service;
    private final ObjectMapper          mapper;

    private static final AtomicInteger THREAD_COUNT = new AtomicInteger();
    private static final ThreadFactory NAMED_DAEMON_FACTORY = r -> {
        Thread t = new Thread(r, "pulsar-feature-" + THREAD_COUNT.incrementAndGet());
        t.setDaemon(true);
        return t;
    };

    // Pool partagé : le mutex dans PulsarFeaturesService empêche les doublons.
    private final ExecutorService executor = Executors.newSingleThreadExecutor(NAMED_DAEMON_FACTORY);

    public PulsarFeaturesController(PulsarFeaturesService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper  = mapper;
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    @GetMapping(value = "/pulsar/key-shared/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @ResponseBody
    public SseEmitter keySharedStream(
            @RequestParam(defaultValue = "300") int messages,
            @RequestParam(defaultValue = "3")   int consumers
    ) {
        SseEmitter emitter = new SseEmitter(120_000L);
        executor.submit(() -> {
            try {
                service.demoKeyShared(messages, consumers, evt -> {
                    try { emitter.send(SseEmitter.event().data(mapper.writeValueAsString(evt))); }
                    catch (Exception e) { emitter.completeWithError(e); }
                });
                emitter.complete();
            } catch (IllegalStateException e) {
                try { emitter.send(SseEmitter.event().name("error").data(e.getMessage())); } catch (Exception ignored) {}
                emitter.complete();
            } catch (Exception e) { emitter.completeWithError(e); }
        });
        return emitter;
    }

    @GetMapping(value = "/pulsar/replay/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @ResponseBody
    public SseEmitter replayStream(
            @RequestParam(defaultValue = "100") int messages
    ) {
        SseEmitter emitter = new SseEmitter(120_000L);
        executor.submit(() -> {
            try {
                service.demoReplay(messages, evt -> {
                    try { emitter.send(SseEmitter.event().data(mapper.writeValueAsString(evt))); }
                    catch (Exception e) { emitter.completeWithError(e); }
                });
                emitter.complete();
            } catch (IllegalStateException e) {
                try { emitter.send(SseEmitter.event().name("error").data(e.getMessage())); } catch (Exception ignored) {}
                emitter.complete();
            } catch (Exception e) { emitter.completeWithError(e); }
        });
        return emitter;
    }

    @GetMapping(value = "/pulsar/dlt/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @ResponseBody
    public SseEmitter dltStream(
            @RequestParam(defaultValue = "18") int messages
    ) {
        SseEmitter emitter = new SseEmitter(120_000L);
        executor.submit(() -> {
            try {
                service.demoDeadLetterTopic(messages, evt -> {
                    try { emitter.send(SseEmitter.event().data(mapper.writeValueAsString(evt))); }
                    catch (Exception e) { emitter.completeWithError(e); }
                });
                emitter.complete();
            } catch (IllegalStateException e) {
                try { emitter.send(SseEmitter.event().name("error").data(e.getMessage())); } catch (Exception ignored) {}
                emitter.complete();
            } catch (Exception e) { emitter.completeWithError(e); }
        });
        return emitter;
    }

    @GetMapping(value = "/pulsar/fanout/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @ResponseBody
    public SseEmitter fanOutStream(
            @RequestParam(defaultValue = "30") int messages
    ) {
        SseEmitter emitter = new SseEmitter(120_000L);
        executor.submit(() -> {
            try {
                service.demoFanOut(messages, evt -> {
                    try { emitter.send(SseEmitter.event().data(mapper.writeValueAsString(evt))); }
                    catch (Exception e) { emitter.completeWithError(e); }
                });
                emitter.complete();
            } catch (IllegalStateException e) {
                try { emitter.send(SseEmitter.event().name("error").data(e.getMessage())); } catch (Exception ignored) {}
                emitter.complete();
            } catch (Exception e) { emitter.completeWithError(e); }
        });
        return emitter;
    }
}
