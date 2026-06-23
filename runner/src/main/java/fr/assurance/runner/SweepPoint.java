package fr.assurance.runner;

public record SweepPoint(
        int payloadBytes,
        String sizeLabel,
        double artemisP99Ms,
        double pulsarP99Ms,
        double artemisThroughputMbSec,
        double pulsarThroughputMbSec
) {}
