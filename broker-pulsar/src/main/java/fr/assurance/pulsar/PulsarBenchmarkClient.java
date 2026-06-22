package fr.assurance.pulsar;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Envoie un message et attend sa réception par le consumer.
     * @return [pubLatencyNs, e2eLatencyNs]  (e2e >= pub)
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
        producer.close();
        if (consumer != null) consumer.close();
        client.close();
    }
}
