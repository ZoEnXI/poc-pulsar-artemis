package fr.assurance.runner;

import java.util.List;

public record SweepProgress(
        String phase,           // "MEASURING" | "POINT" | "DONE"
        int sizeIndex,          // 0-based index de la taille courante
        int totalSizes,
        int payloadBytes,
        String sizeLabel,
        double artemisP99Ms,    // 0 pendant MEASURING
        double pulsarP99Ms,     // 0 pendant MEASURING
        List<SweepPoint> points // points complétés jusqu'ici
) {}
