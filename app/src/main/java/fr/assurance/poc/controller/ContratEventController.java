package fr.assurance.poc.controller;

import fr.assurance.poc.event.ContratEvent;
import fr.assurance.poc.event.ContratEvent.ContratPayload;
import fr.assurance.poc.event.ContratEvent.EventType;
import fr.assurance.poc.producer.ContratEventProducer;
import org.apache.pulsar.client.api.MessageId;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/contrats")
public class ContratEventController {

    private final ContratEventProducer producer;

    public ContratEventController(ContratEventProducer producer) {
        this.producer = producer;
    }

    /**
     * Émet un événement contrat de test.
     *
     * Exemple :
     *   POST /contrats/events
     *   {"contractId":"CTR-001","eventType":"SOUSCRIPTION","montant":50000}
     *
     * Si contractId est absent, un UUID est généré.
     */
    @PostMapping("/events")
    public ResponseEntity<Map<String, String>> emitEvent(@RequestBody EventRequest req) {
        String contractId = req.contractId() != null ? req.contractId() : "CTR-" + UUID.randomUUID();
        EventType type = req.eventType() != null ? req.eventType() : EventType.SOUSCRIPTION;

        ContratEvent event = new ContratEvent(
                contractId,
                type,
                Instant.now(),
                new ContratPayload(
                        req.titulaire() != null ? req.titulaire() : "Titulaire Synthétique",
                        "ASSURANCE-VIE",
                        req.montant() > 0 ? req.montant() : 10000.0,
                        "EUR",
                        "kata-test"
                )
        );

        MessageId msgId = producer.send(event);

        return ResponseEntity.ok(Map.of(
                "contractId", contractId,
                "eventType", type.name(),
                "messageId", msgId.toString()
        ));
    }

    public record EventRequest(
            String contractId,
            EventType eventType,
            String titulaire,
            double montant
    ) {}
}
