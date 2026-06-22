package fr.assurance.runner;

public record BenchmarkParams(
        int warmup,
        int messages,
        int payloadSize,
        boolean artemis,
        boolean pulsar,
        int producerCount,
        int runs             // 1 = streaming par message ; >1 = silent multi-run avec stddev
) {
    public static BenchmarkParams defaults() {
        return new BenchmarkParams(200, 2000, 0, true, true, 1, 1);
    }
}
