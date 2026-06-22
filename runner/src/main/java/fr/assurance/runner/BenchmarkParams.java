package fr.assurance.runner;

public record BenchmarkParams(
        int warmup,
        int messages,
        int payloadSize,
        boolean artemis,
        boolean pulsar,
        int producerCount
) {
    public static BenchmarkParams defaults() {
        return new BenchmarkParams(200, 2000, 0, true, true, 1);
    }
}
