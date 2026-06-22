package fr.assurance.runner;

public record BenchmarkProgress(
        String broker,
        boolean done,
        int sent,
        int total,
        // Latence publication : producteur → ack broker
        double p50Ms,
        double p99Ms,
        double p999Ms,
        double throughputMsgSec,
        // Latence E2E : producteur → réception consommateur (0 si non mesuré)
        double e2eP50Ms,
        double e2eP99Ms,
        double e2eP999Ms
) {}
