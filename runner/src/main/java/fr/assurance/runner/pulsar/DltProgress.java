package fr.assurance.runner.pulsar;

public record DltProgress(
        String phase,       // SENDING | PROCESSING | DLT_DRAINING | DONE
        int sent,
        int total,
        int processed,      // ok messages acked by main consumer
        int inDlt,          // messages received from DLT consumer
        int redeliveries,   // total nack events on main consumer
        int failExpected    // expected DLT message count
) {}
