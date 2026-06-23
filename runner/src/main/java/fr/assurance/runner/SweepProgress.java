package fr.assurance.runner;

import java.util.List;

public record SweepProgress(
        String phase,           // "MEASURING" | "POINT" | "DONE"
        int sizeIndex,          // 0-based index de la taille courante
        int totalSizes,
        int payloadBytes,
        String sizeLabel,
        double artemisP99Ms,
        double pulsarP99Ms,
        double artemisThroughputMbSec,
        double pulsarThroughputMbSec,
        List<SweepPoint> points
) {}
