package fr.assurance.runner;

public record BrokerProperties(
        String mode,
        String artemisUrl,
        String pulsarUrl,
        String artemisDurability,
        String pulsarDurability
) {}
