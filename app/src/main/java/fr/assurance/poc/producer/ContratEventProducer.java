package fr.assurance.poc.producer;

import fr.assurance.poc.event.ContratEvent;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

@Service
public class ContratEventProducer {

    private static final Logger log = LoggerFactory.getLogger(ContratEventProducer.class);

    private static final String TOPIC = "persistent://public/default/contrat-events";

    private final PulsarTemplate<ContratEvent> pulsarTemplate;

    public ContratEventProducer(PulsarTemplate<ContratEvent> pulsarTemplate) {
        this.pulsarTemplate = pulsarTemplate;
    }

    /**
     * Publie un ContratEvent avec contractId comme clé de message.
     * La clé assure le routage vers la même partition (Key_Shared subscription)
     * — cohérent avec le partitionnement par contrat des workloads OMB.
     */
    public MessageId send(ContratEvent event) {
        try {
            MessageId msgId = pulsarTemplate.newMessage(event)
                    .withTopic(TOPIC)
                    .withMessageCustomizer(mb -> mb.key(event.contractId()))
                    .send();
            log.info("Sent contractId={} type={} msgId={}", event.contractId(), event.eventType(), msgId);
            return msgId;
        } catch (Exception e) {
            log.error("Failed to send event contractId={}", event.contractId(), e);
            throw new RuntimeException("Pulsar send failed", e);
        }
    }
}
