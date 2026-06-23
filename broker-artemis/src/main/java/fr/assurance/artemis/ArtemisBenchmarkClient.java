package fr.assurance.artemis;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ArtemisBenchmarkClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ArtemisBenchmarkClient.class);
    static final String QUEUE = "benchmark";

    private final ServerLocator locator;
    private final ClientSessionFactory factory;
    private final ClientSession producerSession;
    private final ClientProducer producer;
    private final ClientSession consumerSession;
    private final ClientConsumer consumer;

    public ArtemisBenchmarkClient(String brokerUrl) throws Exception {
        this(brokerUrl, false);
    }

    /** @param producerOnly si true, aucun consumer — pour les producers parallèles */
    public ArtemisBenchmarkClient(String brokerUrl, boolean producerOnly) throws Exception {
        locator = ActiveMQClient.createServerLocator(brokerUrl)
                .setBlockOnNonDurableSend(true)
                .setBlockOnDurableSend(true);

        factory = locator.createSessionFactory();

        producerSession = factory.createSession(false, true, true);
        try {
            producerSession.createQueue(new SimpleString(QUEUE), new SimpleString(QUEUE), true);
        } catch (ActiveMQException ignored) {}
        producer = producerSession.createProducer(QUEUE);
        producerSession.start();

        if (!producerOnly) {
            consumerSession = factory.createSession(false, true, true);
            consumer = consumerSession.createConsumer(QUEUE);
            consumerSession.start();
        } else {
            consumerSession = null;
            consumer = null;
        }
    }

    /** Latence publish uniquement (ack broker). */
    public long sendAndMeasure(byte[] payload) throws ActiveMQException {
        ClientMessage msg = producerSession.createMessage(true);
        msg.getBodyBuffer().writeBytes(payload);
        long t0 = System.nanoTime();
        producer.send(msg);
        return System.nanoTime() - t0;
    }

    /**
     * Envoie un message et retourne {sendNs, pubLatNs}.
     * Utilisé pour les benchmarks concurrents : le consumer tourne en parallèle
     * sur un thread séparé, sans bloquer le producteur entre chaque message.
     * La sérialisation (writeBytes) est exclue de la mesure.
     */
    public long[] sendAndRecord(byte[] payload) throws ActiveMQException {
        ClientMessage msg = producerSession.createMessage(true);
        msg.getBodyBuffer().writeBytes(payload);
        long t0 = System.nanoTime();
        producer.send(msg);
        return new long[]{t0, System.nanoTime() - t0};
    }

    /**
     * Démarre un thread daemon qui consomme n messages en ordre FIFO.
     * Artemis garantit l'ordre sur une queue single-producer, donc recvNs[i]
     * correspond à sendNs[i] par position.
     * @return Future complété avec un tableau recvNs[i] = nanoTime à la réception.
     */
    public Future<long[]> consumeAsync(int n) {
        if (consumer == null) throw new IllegalStateException("consumeAsync() requires producerOnly=false");
        CompletableFuture<long[]> future = new CompletableFuture<>();
        Thread t = new Thread(() -> {
            long[] recvNs = new long[n];
            int received = 0;
            try {
                while (received < n) {
                    ClientMessage msg = consumer.receive(60_000);
                    if (msg == null) break;
                    recvNs[received++] = System.nanoTime();
                    msg.acknowledge();
                }
                future.complete(recvNs);
            } catch (ActiveMQException e) {
                future.completeExceptionally(e);
            }
        }, "artemis-recv-async");
        t.setDaemon(true);
        t.start();
        return future;
    }

    /**
     * Health check uniquement : envoie 1 message et attend sa réception (stop-and-wait).
     * Ne pas utiliser pour les benchmarks — voir sendAndRecord + consumeAsync.
     * @return [pubLatencyNs, e2eLatencyNs]
     */
    public long[] sendAndMeasureBoth(byte[] payload) throws ActiveMQException {
        if (consumer == null) throw new IllegalStateException("sendAndMeasureBoth() requires producerOnly=false");
        ClientMessage msg = producerSession.createMessage(true);
        msg.getBodyBuffer().writeBytes(payload);
        long sentAt = System.nanoTime();
        producer.send(msg);
        long pubLat = System.nanoTime() - sentAt;

        try {
            ClientMessage received = consumer.receive(5_000);
            long e2eLat = System.nanoTime() - sentAt;
            if (received != null) received.acknowledge();
            return new long[]{pubLat, Math.max(pubLat, e2eLat)};
        } catch (ActiveMQException e) {
            return new long[]{pubLat, pubLat};
        }
    }

    /** Vide la queue après la phase de warmup (utilise setMessageHandler). */
    public int drain(int expected, long timeoutMs) throws Exception {
        if (consumer == null) throw new IllegalStateException("drain() requires producerOnly=false");
        CountDownLatch latch = new CountDownLatch(expected);
        consumer.setMessageHandler(msg -> {
            try { msg.acknowledge(); } catch (ActiveMQException ignored) {}
            latch.countDown();
        });
        latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        consumer.setMessageHandler(null);
        return expected - (int) latch.getCount();
    }

    @Override
    public void close() throws Exception {
        try { producer.close(); } finally {
            try { producerSession.close(); } finally {
                try { if (consumer != null) consumer.close(); } finally {
                    try { if (consumerSession != null) consumerSession.close(); } finally {
                        try { factory.close(); } finally {
                            locator.close();
                        }
                    }
                }
            }
        }
    }
}
