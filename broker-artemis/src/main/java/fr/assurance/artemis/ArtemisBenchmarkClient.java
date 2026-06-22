package fr.assurance.artemis;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
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
     * Envoie un message et attend sa réception par le consumer.
     * @return [pubLatencyNs, e2eLatencyNs]  (e2e >= pub)
     */
    public long[] sendAndMeasureBoth(byte[] payload) throws ActiveMQException {
        if (consumer == null) throw new IllegalStateException("sendAndMeasureBoth() requires producerOnly=false");
        ClientMessage msg = producerSession.createMessage(true);
        long sentAt = System.nanoTime();
        msg.getBodyBuffer().writeBytes(payload);
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
        producer.close();
        producerSession.close();
        if (consumer != null) consumer.close();
        if (consumerSession != null) consumerSession.close();
        factory.close();
        locator.close();
    }
}
