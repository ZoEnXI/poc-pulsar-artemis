package fr.assurance.runner.pulsar;

public record ReplayProgress(
        String phase,
        int progress,
        int total,
        int replayCount
) {}
