package fr.assurance.runner.pulsar;

import fr.assurance.pulsar.EmbeddedPulsarServer;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

@Service
public class PulsarFeaturesService {

    private static final Logger log = LoggerFactory.getLogger(PulsarFeaturesService.class);
    private static final String[] KEYS = {"CTR-A", "CTR-B", "CTR-C", "CTR-D", "CTR-E", "CTR-F"};
    private static final String DEMO_NS = "public/demo";

    private final EmbeddedPulsarServer pulsarServer;

    public PulsarFeaturesService(EmbeddedPulsarServer pulsarServer) {
        this.pulsarServer = pulsarServer;
    }

    // ── Key_Shared demo ───────────────────────────────────────────────────────

    /**
     * Démonstration Key_Shared : N consumers partagent le même topic/subscription,
     * chaque clé métier est toujours routée vers le MÊME consumer (garantie d'ordre par entité).
     */
    public void demoKeyShared(int messages, int numConsumers,
                              Consumer<KeySharedProgress> onEvent) throws Exception {
        String topic = "persistent://" + DEMO_NS + "/ks-" + System.currentTimeMillis();
        String sub   = "ks-sub";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarServer.getBrokerUrl())
                .build();
        try {
            // Tous les consumers doivent être créés AVANT le producteur
            // pour que le broker calcule les plages de hash une seule fois.
            List<org.apache.pulsar.client.api.Consumer<byte[]>> consumers = new ArrayList<>();
            for (int c = 0; c < numConsumers; c++) {
                consumers.add(client.newConsumer()
                        .topic(topic)
                        .subscriptionName(sub)
                        .subscriptionType(SubscriptionType.Key_Shared)
                        .subscribe());
            }
            Thread.sleep(400); // laisser le broker finaliser l'assignation des plages

            ConcurrentHashMap<String, Integer> assignments = new ConcurrentHashMap<>();
            AtomicInteger[] counts = new AtomicInteger[numConsumers];
            for (int i = 0; i < numConsumers; i++) counts[i] = new AtomicInteger();
            AtomicInteger totalReceived = new AtomicInteger();
            AtomicInteger violations    = new AtomicInteger();
            AtomicBoolean running       = new AtomicBoolean(true);
            CountDownLatch latch        = new CountDownLatch(messages);

            ExecutorService pool = Executors.newFixedThreadPool(numConsumers);
            for (int c = 0; c < numConsumers; c++) {
                final int cid  = c;
                final org.apache.pulsar.client.api.Consumer<byte[]> cons = consumers.get(c);
                pool.submit(() -> {
                    while (running.get()) {
                        try {
                            Message<byte[]> msg = cons.receive(300, TimeUnit.MILLISECONDS);
                            if (msg == null) continue;
                            String key = msg.getKey();
                            Integer prev = assignments.put(key, cid);
                            if (prev != null && !prev.equals(cid)) violations.incrementAndGet();
                            counts[cid].incrementAndGet();
                            totalReceived.incrementAndGet();
                            cons.acknowledge(msg);
                            latch.countDown();
                        } catch (Exception e) {
                            if (running.get()) log.debug("KS consumer {}: {}", cid, e.getMessage());
                        }
                    }
                });
            }

            Producer<byte[]> prod = client.newProducer()
                    .topic(topic)
                    .enableBatching(false)
                    .create();

            for (int i = 0; i < messages; i++) {
                String key = KEYS[i % KEYS.length];
                prod.newMessage().key(key).value(("m" + i).getBytes()).send();
                if ((i + 1) % 30 == 0 || i == messages - 1)
                    onEvent.accept(new KeySharedProgress("progress", i + 1, messages,
                            totalReceived.get(), Map.copyOf(assignments),
                            toArray(counts), violations.get()));
            }
            prod.close();

            latch.await(30, TimeUnit.SECONDS);
            running.set(false);
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);

            onEvent.accept(new KeySharedProgress("done", messages, messages,
                    totalReceived.get(), Map.copyOf(assignments),
                    toArray(counts), violations.get()));

            for (var c : consumers) { try { c.close(); } catch (Exception ignored) {} }
        } finally {
            client.close();
        }
    }

    // ── Message Replay demo ───────────────────────────────────────────────────

    /**
     * Démonstration Message Replay : les messages sont acquittés, puis le consumer
     * effectue un seek(earliest) pour les relire depuis le début.
     * Possible grâce à la rétention configurée sur public/demo (60 min / 100 MB).
     */
    public void demoReplay(int messages, Consumer<ReplayProgress> onEvent) throws Exception {
        String topic = "persistent://" + DEMO_NS + "/replay-" + System.currentTimeMillis();
        String sub   = "replay-sub";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarServer.getBrokerUrl())
                .build();
        try {
            org.apache.pulsar.client.api.Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName(sub)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe();

            Producer<byte[]> prod = client.newProducer()
                    .topic(topic)
                    .enableBatching(false)
                    .create();

            // Phase SENDING
            for (int i = 0; i < messages; i++) {
                prod.newMessage().value(("replay-" + i).getBytes()).send();
                if ((i + 1) % 20 == 0 || i == messages - 1)
                    onEvent.accept(new ReplayProgress("SENDING", i + 1, messages, 0));
            }
            prod.close();

            // Phase CONSUMING — première lecture, on acquitte tout
            int consumed = 0;
            while (consumed < messages) {
                Message<byte[]> msg = consumer.receive(3, TimeUnit.SECONDS);
                if (msg == null) break;
                consumer.acknowledge(msg);
                consumed++;
                if (consumed % 20 == 0 || consumed == messages)
                    onEvent.accept(new ReplayProgress("CONSUMING", consumed, messages, 0));
            }

            // Phase SEEKING — le seek remet le curseur au tout début du topic
            onEvent.accept(new ReplayProgress("SEEKING", consumed, messages, 0));
            consumer.seek(MessageId.earliest);
            Thread.sleep(300);

            // Phase REPLAYING — tous les messages sont disponibles grâce à la rétention
            int replayed = 0;
            while (replayed < messages) {
                Message<byte[]> msg = consumer.receive(3, TimeUnit.SECONDS);
                if (msg == null) break;
                consumer.acknowledge(msg);
                replayed++;
                if (replayed % 20 == 0 || replayed == messages)
                    onEvent.accept(new ReplayProgress("REPLAYING", replayed, messages, 0));
            }

            onEvent.accept(new ReplayProgress("DONE", messages, messages, replayed));
            consumer.close();
        } finally {
            client.close();
        }
    }

    private static int[] toArray(AtomicInteger[] ai) {
        int[] r = new int[ai.length];
        for (int i = 0; i < ai.length; i++) r[i] = ai[i].get();
        return r;
    }
}
