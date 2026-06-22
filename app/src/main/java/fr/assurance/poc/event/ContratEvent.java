package fr.assurance.poc.event;

import java.time.Instant;

/**
 * Événement contrat assurance — schéma canonique du POC.
 *
 * Utilisé comme schéma JSON enregistré dans le Schema Registry de Pulsar.
 * La clé de message = contractId (partitionnement par contrat dans OMB workload A).
 */
public record ContratEvent(
        String contractId,
        EventType eventType,
        Instant timestamp,
        ContratPayload payload
) {

    public enum EventType {
        SOUSCRIPTION,
        AVENANT,
        RACHAT,
        SINISTRE
    }

    /**
     * Payload métier synthétique — structure représentative, pas de données réelles.
     */
    public record ContratPayload(
            String titulaire,
            String produit,
            double montant,
            String devise,
            String metadata
    ) {}
}
