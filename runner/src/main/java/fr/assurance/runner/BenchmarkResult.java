package fr.assurance.runner;

public record BenchmarkResult(
        BrokerMetrics artemis,
        BrokerMetrics pulsar
) {
    public record BrokerMetrics(
            int messagesSent,
            double p50Ms,
            double p99Ms,
            double p999Ms,
            double throughputMsgSec,
            double e2eP50Ms,
            double e2eP99Ms,
            double e2eP999Ms
    ) {}
}
