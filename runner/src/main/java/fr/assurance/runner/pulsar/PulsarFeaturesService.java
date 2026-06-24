package fr.assurance.runner.pulsar;

import fr.assurance.runner.BrokerProperties;
import jakarta.annotation.PostConstruct;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
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

    private final BrokerProperties brokers;

    // Mutex : une seule démo Pulsar à la fois pour isoler les mesures
    private final AtomicBoolean featureRunning = new AtomicBoolean(false);

    public PulsarFeaturesService(BrokerProperties brokers) {
        this.brokers = brokers;
    }

    /**
     * En mode external (Docker), le namespace public/demo n'existe pas par défaut.
     * On le crée via PulsarAdmin HTTP avant la première démo.
     * En mode embedded, le namespace est déjà initialisé par EmbeddedPulsarServer.
     */
    @PostConstruct
    public void initDemoNamespace() {
        String adminUrl = brokers.pulsarAdminUrl();
        if (adminUrl == null || adminUrl.isBlank()) return;
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            try {
                admin.namespaces().createNamespace("public/demo");
                admin.namespaces().setRetention("public/demo", new RetentionPolicies(60, 100));
                log.info("Namespace 'public/demo' créé via admin HTTP (mode external)");
            } catch (PulsarAdminException.ConflictException ignored) {
                log.debug("Namespace 'public/demo' existe déjà");
            }
        } catch (Exception e) {
            log.warn("Impossible d'initialiser public/demo via admin HTTP : {}", e.getMessage());
        }
    }

    private String uniqueTopic(String prefix) {
        return "persistent://" + DEMO_NS + "/" + prefix + "-"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    // ── Key_Shared demo ───────────────────────────────────────────────────────

    public void demoKeyShared(int messages, int numConsumers,
                              Consumer<KeySharedProgress> onEvent) throws Exception {
        if (!featureRunning.compareAndSet(false, true))
            throw new IllegalStateException("Une démo Pulsar est déjà en cours — réessayez dans quelques instants.");
        try {
            String topic = uniqueTopic("ks");
            String sub   = "ks-sub";

            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(brokers.pulsarUrl())
                    .build();
            try {
                // Tous les consumers doivent être créés AVANT le producteur
                List<org.apache.pulsar.client.api.Consumer<byte[]>> consumers = new ArrayList<>();
                for (int c = 0; c < numConsumers; c++) {
                    consumers.add(client.newConsumer()
                            .topic(topic)
                            .subscriptionName(sub)
                            .subscriptionType(SubscriptionType.Key_Shared)
                            .subscribe());
                }
                Thread.sleep(400);

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
                                if (key == null) { cons.acknowledge(msg); continue; } // guard NPE Map.copyOf
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
        } finally {
            featureRunning.set(false);
        }
    }

    // ── Message Replay demo ───────────────────────────────────────────────────

    /**
     * Démonstration Message Replay : les messages sont acquittés puis relus depuis
     * le début via l'API Reader, qui lit directement depuis le ledger sans dépendre
     * de la rétention basée sur les acknowledgements.
     */
    public void demoReplay(int messages, Consumer<ReplayProgress> onEvent) throws Exception {
        if (!featureRunning.compareAndSet(false, true))
            throw new IllegalStateException("Une démo Pulsar est déjà en cours — réessayez dans quelques instants.");
        try {
            String topic = uniqueTopic("replay");
            String sub   = "replay-sub";

            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(brokers.pulsarUrl())
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
                consumer.close();

                // Phase REPLAYING — via Reader : lit directement depuis le ledger (pas de seek sur curseur acquitté)
                onEvent.accept(new ReplayProgress("SEEKING", consumed, messages, 0));

                Reader<byte[]> reader = client.newReader()
                        .topic(topic)
                        .startMessageId(MessageId.earliest)
                        .create();

                int replayed = 0;
                while (replayed < messages) {
                    Message<byte[]> msg = reader.readNext(3, TimeUnit.SECONDS);
                    if (msg == null) break;
                    replayed++;
                    if (replayed % 20 == 0 || replayed == messages)
                        onEvent.accept(new ReplayProgress("REPLAYING", replayed, messages, 0));
                }
                reader.close();

                onEvent.accept(new ReplayProgress("DONE", messages, messages, replayed));
            } finally {
                client.close();
            }
        } finally {
            featureRunning.set(false);
        }
    }

    // ── Dead Letter Topic demo ────────────────────────────────────────────────

    public void demoDeadLetterTopic(int messages, Consumer<DltProgress> onEvent) throws Exception {
        if (!featureRunning.compareAndSet(false, true))
            throw new IllegalStateException("Une démo Pulsar est déjà en cours — réessayez dans quelques instants.");
        try {
            String uid      = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            String topic    = "persistent://" + DEMO_NS + "/dlt-" + uid;
            String sub      = "main-sub";
            String dltTopic = "persistent://" + DEMO_NS + "/dlt-dlq-" + uid;

            final int BAD_PERIOD      = 3;
            final int MAX_REDELIVER   = 2;
            final int failExpected    = (int) Math.ceil(messages / (double) BAD_PERIOD);
            final int goodExpected    = messages - failExpected;
            final int totalDeliveries = goodExpected + failExpected * (MAX_REDELIVER + 1);

            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(brokers.pulsarUrl())
                    .build();
            try {
                org.apache.pulsar.client.api.Consumer<byte[]> dltConsumer = client.newConsumer()
                        .topic(dltTopic)
                        .subscriptionName("dlt-reader")
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscribe();

                org.apache.pulsar.client.api.Consumer<byte[]> mainConsumer = client.newConsumer()
                        .topic(topic)
                        .subscriptionName(sub)
                        .subscriptionType(SubscriptionType.Exclusive)
                        .negativeAckRedeliveryDelay(400, TimeUnit.MILLISECONDS)
                        .deadLetterPolicy(DeadLetterPolicy.builder()
                                .maxRedeliverCount(MAX_REDELIVER)
                                .deadLetterTopic(dltTopic)
                                .build())
                        .subscribe();

                Producer<byte[]> prod = client.newProducer()
                        .topic(topic)
                        .enableBatching(false)
                        .create();
                for (int i = 0; i < messages; i++) {
                    boolean bad = (i % BAD_PERIOD == 0);
                    prod.newMessage()
                            .key(bad ? "FAIL" : "OK")
                            .value((bad ? "fail-" : "ok-").getBytes())
                            .send();
                    if ((i + 1) % 5 == 0 || i == messages - 1)
                        onEvent.accept(new DltProgress("SENDING", i + 1, messages, 0, 0, 0, failExpected));
                }
                prod.close();

                int processed = 0, redeliveries = 0, deliveries = 0;
                while (deliveries < totalDeliveries) {
                    Message<byte[]> msg = mainConsumer.receive(5, TimeUnit.SECONDS);
                    if (msg == null) break;
                    deliveries++;
                    String payload = new String(msg.getValue());
                    if (payload.startsWith("fail")) {
                        mainConsumer.negativeAcknowledge(msg);
                        redeliveries++;
                    } else {
                        mainConsumer.acknowledge(msg);
                        processed++;
                    }
                    onEvent.accept(new DltProgress("PROCESSING", messages, messages,
                            processed, 0, redeliveries, failExpected));
                }
                mainConsumer.close();

                onEvent.accept(new DltProgress("DLT_DRAINING", messages, messages,
                        processed, 0, redeliveries, failExpected));
                int inDlt = 0;
                while (inDlt < failExpected) {
                    Message<byte[]> msg = dltConsumer.receive(8, TimeUnit.SECONDS);
                    if (msg == null) break;
                    dltConsumer.acknowledge(msg);
                    inDlt++;
                    onEvent.accept(new DltProgress("DLT_DRAINING", messages, messages,
                            processed, inDlt, redeliveries, failExpected));
                }
                dltConsumer.close();

                onEvent.accept(new DltProgress("DONE", messages, messages,
                        processed, inDlt, redeliveries, failExpected));
            } finally {
                client.close();
            }
        } finally {
            featureRunning.set(false);
        }
    }

    // ── Fan-out multi-subscription demo ──────────────────────────────────────

    public void demoFanOut(int messages, Consumer<FanOutProgress> onEvent) throws Exception {
        if (!featureRunning.compareAndSet(false, true))
            throw new IllegalStateException("Une démo Pulsar est déjà en cours — réessayez dans quelques instants.");
        try {
            String topic = uniqueTopic("fanout");

            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(brokers.pulsarUrl())
                    .build();
            try {
                org.apache.pulsar.client.api.Consumer<byte[]> auditConsumer = client.newConsumer()
                        .topic(topic)
                        .subscriptionName("audit-trail")
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscribe();

                org.apache.pulsar.client.api.Consumer<byte[]> analyticsConsumer = client.newConsumer()
                        .topic(topic)
                        .subscriptionName("analytics-engine")
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscribe();

                Thread.sleep(300);

                Producer<byte[]> prod = client.newProducer()
                        .topic(topic)
                        .enableBatching(false)
                        .create();
                for (int i = 0; i < messages; i++) {
                    prod.newMessage().value(("event-" + i).getBytes()).send();
                    if ((i + 1) % 10 == 0 || i == messages - 1)
                        onEvent.accept(new FanOutProgress("sending", i + 1, messages, 0, 0));
                }
                prod.close();

                int c1 = 0, c2 = 0;
                while (c1 < messages || c2 < messages) {
                    if (c1 < messages) {
                        Message<byte[]> m = auditConsumer.receive(200, TimeUnit.MILLISECONDS);
                        if (m != null) { auditConsumer.acknowledge(m); c1++; }
                    }
                    if (c2 < messages) {
                        Message<byte[]> m = analyticsConsumer.receive(200, TimeUnit.MILLISECONDS);
                        if (m != null) { analyticsConsumer.acknowledge(m); c2++; }
                    }
                    if (c1 > 0 || c2 > 0)
                        onEvent.accept(new FanOutProgress("progress", messages, messages, c1, c2));
                }

                onEvent.accept(new FanOutProgress("done", messages, messages, c1, c2));
                auditConsumer.close();
                analyticsConsumer.close();
            } finally {
                client.close();
            }
        } finally {
            featureRunning.set(false);
        }
    }

    private static int[] toArray(AtomicInteger[] ai) {
        int[] r = new int[ai.length];
        for (int i = 0; i < ai.length; i++) r[i] = ai[i].get();
        return r;
    }
}
