package fr.assurance.poc.consumer;

import fr.assurance.poc.event.ContratEvent;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Component;

@Component
public class ContratEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ContratEventConsumer.class);

    /**
     * Consomme les événements contrats en mode Key_Shared.
     *
     * Key_Shared garantit :
     *   - Les messages d'un même contractId sont livrés au même consommateur
     *     (ordre préservé par clé, comme en partition Kafka)
     *   - Plusieurs instances peuvent consommer en parallèle sans collision
     *     → démontrable via le coding kata (lancer 2 instances, observer le routage)
     *
     * Le schema JSON est enregistré automatiquement dans le Schema Registry
     * Pulsar à la première connexion (spring-pulsar auto-schema).
     */
    @PulsarListener(
            topics = "persistent://public/default/contrat-events",
            subscriptionName = "insurance-app-sub",
            subscriptionType = SubscriptionType.Key_Shared,
            schemaType = org.apache.pulsar.common.schema.SchemaType.JSON
    )
    public void onContratEvent(ContratEvent event) {
        log.info("[CONSUMER] contractId={} type={} ts={} montant={}{}",
                event.contractId(),
                event.eventType(),
                event.timestamp(),
                event.payload().montant(),
                event.payload().devise());
    }
}
