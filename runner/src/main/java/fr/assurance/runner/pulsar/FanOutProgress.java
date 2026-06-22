package fr.assurance.runner.pulsar;

public record FanOutProgress(
        String type,        // "sending" | "progress" | "done"
        int sent,
        int total,
        int sub1Received,   // "audit-trail" subscription
        int sub2Received    // "analytics-engine" subscription
) {}
