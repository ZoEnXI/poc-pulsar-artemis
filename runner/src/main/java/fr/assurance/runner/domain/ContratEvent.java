package fr.assurance.runner.domain;

import java.time.Instant;

public record ContratEvent(
        String contractId,
        EventType eventType,
        Instant timestamp,
        String titulaire,
        String produit,
        double montant
) {
    public enum EventType {
        SOUSCRIPTION, AVENANT, RACHAT, SINISTRE
    }
}
