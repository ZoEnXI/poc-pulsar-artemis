package fr.assurance.runner.pulsar;

import java.util.Map;

public record KeySharedProgress(
        String type,
        int sent,
        int total,
        int received,
        Map<String, Integer> assignments,
        int[] counts,
        int violations
) {}
