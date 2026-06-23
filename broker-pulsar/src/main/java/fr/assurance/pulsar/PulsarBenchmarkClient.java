package fr.assurance.pulsar;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PulsarBenchmarkClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PulsarBenchmarkClient.class);
    static final String TOPIC = "persistent://public/default/benchmark";
    private static final String SUB   = "benchmark-sub";

    private final PulsarClient client;
    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;

    public PulsarBenchmarkClient(String brokerUrl) throws PulsarClientException {
        this(brokerUrl, false);
    }

    /** @param producerOnly si true, aucun consumer — pour les producers parallèles */
    public PulsarBenchmarkClient(String brokerUrl, boolean producerOnly) throws PulsarClientException {
        client = PulsarClient.builder().serviceUrl(brokerUrl).build();

        producer = client.newProducer()
                .topic(TOPIC)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .sendTimeout(30, TimeUnit.SECONDS)
                .create();

        if (!producerOnly) {
            consumer = client.newConsumer()
                    .topic(TOPIC)
                    .subscriptionName(SUB)
                    .subscriptionType(SubscriptionType.Shared)
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
                    Message<byte[]> m = consumer.receive(60, TimeUnit.SECONDS);
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

    public int drain(int expected, long timeoutMs) throws PulsarClientException {
        if (consumer == null) throw new IllegalStateException("drain() requires producerOnly=false");
        int count = 0;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (count < expected && System.currentTimeMillis() < deadline) {
            Message<byte[]> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            if (msg != null) { consumer.acknowledge(msg); count++; }
        }
        return count;
    }

    @Override
    public void close() throws PulsarClientException {
        try { producer.close(); } finally {
            try { if (consumer != null) consumer.close(); } finally {
                client.close();
            }
        }
    }
}
