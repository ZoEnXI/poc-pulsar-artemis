package fr.assurance.artemis;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

public class EmbeddedArtemisServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedArtemisServer.class);

    private EmbeddedActiveMQ server;
    private int port;

    public void start() throws Exception {
        port = freePort();

        // Journal dans un tmpdir : persistance sur disque sans fsync forcé
        // → iso-durable avec Pulsar/BookKeeper standalone (qui n'impose pas non plus le fsync par défaut)
        Path journalDir = Files.createTempDirectory("artemis-journal-");

        Configuration config = new ConfigurationImpl()
                .setPersistenceEnabled(true)
                .setJournalDirectory(journalDir.toString())
                .setBindingsDirectory(journalDir.resolve("bindings").toString())
                .setLargeMessagesDirectory(journalDir.resolve("large").toString())
                .setJournalSyncNonTransactional(false)   // pas de fsync sur les writes non-tx
                .setJournalSyncTransactional(false)       // idem tx (cohérent avec BK standalone)
                .setSecurityEnabled(false)
                .addAcceptorConfiguration("core", "tcp://localhost:" + port + "?protocols=CORE");

        server = new EmbeddedActiveMQ().setConfiguration(config);
        server.start();

        log.info("Artemis embedded started on port {} (journal: {})", port, journalDir);
    }

    public int getPort() { return port; }

    public String getBrokerUrl() { return "tcp://localhost:" + port; }

    /** true = journal activé sur tmpdir (mode courant) */
    public boolean isPersistent() { return true; }

    @Override
    public void close() throws Exception {
        if (server != null) {
            server.stop();
            log.info("Artemis embedded stopped");
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }
}
