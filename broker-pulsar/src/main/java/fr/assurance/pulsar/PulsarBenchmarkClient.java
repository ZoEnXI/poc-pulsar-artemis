package fr.assurance.pulsar;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PulsarBenchmarkClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PulsarBenchmarkClient.class);
    static final String TOPIC = "persistent://public/default/benchmark";
    private static final String SUB   = "benchmark-sub";

    private final PulsarClient client;
    private final boolean ownsClient;
    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;

    /** Crée et possède son propre PulsarClient (health check, sweep legacy). */
    public PulsarBenchmarkClient(String brokerUrl) throws PulsarClientException {
        this(brokerUrl, false);
    }

    /** Crée et possède son propre PulsarClient. */
    public PulsarBenchmarkClient(String brokerUrl, boolean producerOnly) throws PulsarClientException {
        this(PulsarClient.builder().serviceUrl(brokerUrl).build(), producerOnly, true);
    }

    /**
     * Utilise un PulsarClient existant (singleton) — ne le ferme PAS dans close().
     * Permet de réutiliser le pool Netty et les connexions TCP entre les runs.
     */
    public PulsarBenchmarkClient(PulsarClient client, boolean producerOnly) throws PulsarClientException {
        this(client, producerOnly, false);
    }

    private PulsarBenchmarkClient(PulsarClient client, boolean producerOnly, boolean ownsClient)
            throws PulsarClientException {
        this.client = client;
        this.ownsClient = ownsClient;

        // maxPendingMessages(1) : force 1 message en vol à la fois.
        // Sans ça, le producer Pulsar pipeline jusqu'à 1000 asyncAddEntry BK simultanés.
        // Sur 5 runs × 2200 msgs, le bookie (unique, embedded) accumule ~10 000 pending
        // → ETOOMANYREQUESTS. Avec 1, il ne peut jamais y en avoir plus d'1 en file.
        // Effet positif pour le bench latence : on mesure la latence d'un seul message,
        // pas le débit pipeliné.
        producer = client.newProducer()
                .topic(TOPIC)
                .enableBatching(false)
                .maxPendingMessages(1)
                .blockIfQueueFull(true)
                .sendTimeout(120, TimeUnit.SECONDS)
                .create();

        if (!producerOnly) {
            consumer = client.newConsumer()
                    .topic(TOPIC)
                    .subscriptionName(SUB)
                    .subscriptionType(SubscriptionType.Shared)
                    // NonDurable = curseur en mémoire uniquement : ZERO cursor-write BK.
                    // Sans ça, chaque consumer.acknowledge() déclenche un write async sur
                    // le bookie embedded → ETOOMANYREQUESTS sur la sweep (8000+ acks).
                    // La subscription disparaît automatiquement à la fermeture du consumer
                    // → pas de backlog résiduel entre runs, unsubscribe() devient défensif.
                    .subscriptionMode(SubscriptionMode.NonDurable)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                    .subscribe();
        } else {
            consumer = null;
        }
    }

    /** Latence publish uniquement (ack broker). */
    public long sendAndMeasure(byte[] payload, String key) throws PulsarClientException {
        long t0 = System.nanoTime();
        producer.newMessage().key(key).value(payload).send();
        return System.nanoTime() - t0;
    }

    /**
     * Envoie un message et retourne {sendNs, pubLatNs}.
     * Utilisé pour les benchmarks concurrents : le consumer tourne en parallèle
     * sur un thread séparé, sans bloquer le producteur entre chaque message.
     */
    public long[] sendAndRecord(byte[] payload, String key) throws PulsarClientException {
        long t0 = System.nanoTime();
        producer.newMessage().key(key).value(payload).send();
        return new long[]{t0, System.nanoTime() - t0};
    }

    /**
     * Démarre un thread daemon qui consomme n messages.
     * Chaque message doit avoir une key parseable en entier (seqno dans [0, n-1]).
     * @return Future complété avec un tableau recvNs[seqno] = nanoTime à la réception.
     */
    public Future<long[]> consumeAsync(int n) {
        return consumeAsync(n, new long[n]);
    }

    /**
     * Variante avec tableau externe pré-alloué par l'appelant.
     * Permet des lectures partielles par le thread producteur (E2E progressif) :
     * recvNs[seq] > 0 ⟺ message seq déjà reçu. Pas de garantie mémoire stricte
     * entre threads, mais acceptable pour des métriques de progression (POC).
     */
    public Future<long[]> consumeAsync(int n, long[] recvNs) {
        if (consumer == null) throw new IllegalStateException("consumeAsync() requires producerOnly=false");
        CompletableFuture<long[]> future = new CompletableFuture<>();
        Thread t = new Thread(() -> {
            int received = 0;
            try {
                while (received < n) {
                    Message<byte[]> m = consumer.receive(120, TimeUnit.SECONDS);
                    if (m == null) break;
                    long recvTime = System.nanoTime(); // capture AVANT acknowledge
                    consumer.acknowledge(m);
                    try {
                        int seq = Integer.parseInt(m.getKey());
                        if (seq >= 0 && seq < n) {
                            recvNs[seq] = recvTime;
                            received++;
                        }
                    } catch (NumberFormatException ignored) { }
                }
                future.complete(recvNs);
            } catch (PulsarClientException e) {
                future.completeExceptionally(e);
            }
        }, "pulsar-recv-async");
        t.setDaemon(true);
        t.start();
        return future;
    }

    /**
     * Health check uniquement : envoie 1 message et attend sa réception (stop-and-wait).
     * Ne pas utiliser pour les benchmarks — voir sendAndRecord + consumeAsync.
     */
    public long[] sendAndMeasureBoth(byte[] payload, String key) throws PulsarClientException {
        if (consumer == null) throw new IllegalStateException("sendAndMeasureBoth() requires producerOnly=false");
        long sentAt = System.nanoTime();
        producer.newMessage().key(key).value(payload).send();
        long pubLat = System.nanoTime() - sentAt;

        Message<byte[]> received = consumer.receive(5, TimeUnit.SECONDS);
        long e2eLat = System.nanoTime() - sentAt;
        if (received != null) consumer.acknowledge(received);
        return new long[]{pubLat, Math.max(pubLat, e2eLat)};
    }

    /**
     * Fix #4 : drain via receiveAsync() au lieu du polling fixe à 100ms.
     * receiveAsync() retourne dès qu'un message est disponible — le seul délai
     * observé est le temps réseau réel, pas un timer arbitraire.
     */
    public int drain(int expected, long timeoutMs) throws PulsarClientException {
        if (consumer == null) throw new IllegalStateException("drain() requires producerOnly=false");
        if (expected <= 0) return 0;
        int count = 0;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (count < expected) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;
            try {
                Message<byte[]> msg = consumer.receiveAsync()
                        .get(remaining, TimeUnit.MILLISECONDS);
                consumer.acknowledgeAsync(msg);
                count++;
            } catch (TimeoutException e) {
                break;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof PulsarClientException pce) throw pce;
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return count;
    }

    /**
     * Fix #1 : unsubscribe() supprime le curseur côté broker avant de fermer le consumer.
     * Les messages non-ack ne seront plus redelivrés au run suivant.
     * Fix #3 : client.close() uniquement si ce client possède la connexion (ownsClient).
     */
    @Override
    public void close() throws PulsarClientException {
        try {
            producer.close();
        } finally {
            try {
                if (consumer != null) {
                    try { consumer.unsubscribe(); } catch (PulsarClientException ignored) {}
                    consumer.close();
                }
            } finally {
                if (ownsClient) client.close();
            }
        }
    }
}
