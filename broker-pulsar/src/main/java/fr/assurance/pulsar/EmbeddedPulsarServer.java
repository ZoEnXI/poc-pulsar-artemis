package fr.assurance.pulsar;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.Set;

/**
 * Démarre un Pulsar standalone in-process :
 *   ZooKeeper embedded → BookKeeper embedded → PulsarService (broker)
 *
 * Approche identique à celle des tests internes d'Apache Pulsar.
 * Durée de démarrage : 5-15 s selon la machine.
 */
public class EmbeddedPulsarServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedPulsarServer.class);

    private LocalBookkeeperEnsemble bkEnsemble;
    private PulsarService pulsarService;
    private int brokerPort;
    private int webPort;

    public void start() throws Exception {
        int zkPort  = freePort();
        int bkPort  = freePort();
        brokerPort  = freePort();
        webPort     = freePort();

        log.info("Starting ZooKeeper + BookKeeper on zk={} bk={}", zkPort, bkPort);
        bkEnsemble = new LocalBookkeeperEnsemble(1, zkPort, () -> bkPort);
        bkEnsemble.start();

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setClusterName("standalone");
        conf.setAdvertisedAddress("localhost");
        conf.setZookeeperServers("127.0.0.1:" + zkPort);
        conf.setConfigurationStoreServers("127.0.0.1:" + zkPort);
        conf.setBrokerServicePort(Optional.of(brokerPort));
        conf.setWebServicePort(Optional.of(webPort));

        // Quorum mono-bookie
        conf.setManagedLedgerDefaultEnsembleSize(1);
        conf.setManagedLedgerDefaultWriteQuorum(1);
        conf.setManagedLedgerDefaultAckQuorum(1);

        // Désactiver les composants non nécessaires pour le POC
        conf.setFunctionsWorkerEnabled(false);
        conf.setTransactionCoordinatorEnabled(false);

        conf.setAllowAutoTopicCreation(true);
        conf.setAllowAutoTopicCreationType(
                org.apache.pulsar.common.policies.data.TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(1);

        // Tenant + namespace créés au démarrage
        conf.setClusterName("standalone");
        conf.setSuperUserRoles(Set.of("admin"));
        conf.setAuthenticationEnabled(false);
        conf.setAuthorizationEnabled(false);

        log.info("Starting Pulsar broker on port {}", brokerPort);
        pulsarService = new PulsarService(conf);
        pulsarService.start();

        // Initialiser tenant public + namespace default (comme pulsar standalone)
        initNamespace();

        log.info("Pulsar embedded ready — brokerUrl={}", getBrokerUrl());
    }

    public String getBrokerUrl() {
        return "pulsar://localhost:" + brokerPort;
    }

    public String getAdminUrl() {
        return "http://localhost:" + webPort;
    }

    @Override
    public void close() throws Exception {
        if (pulsarService != null) {
            pulsarService.close();
        }
        if (bkEnsemble != null) {
            bkEnsemble.stop();
        }
        log.info("Pulsar embedded stopped");
    }

    private void initNamespace() {
        try {
            var admin = pulsarService.getAdminClient();
            if (!admin.tenants().getTenants().contains("public")) {
                admin.tenants().createTenant("public",
                        org.apache.pulsar.common.policies.data.TenantInfo.builder()
                                .allowedClusters(Set.of("standalone"))
                                .build());
            }
            var namespaces = admin.namespaces().getNamespaces("public");
            if (!namespaces.contains("public/default")) {
                admin.namespaces().createNamespace("public/default");
            }
            // Namespace dédié aux démos de features — retention activée pour le replay demo
            if (!namespaces.contains("public/demo")) {
                admin.namespaces().createNamespace("public/demo");
                admin.namespaces().setRetention("public/demo",
                        new org.apache.pulsar.common.policies.data.RetentionPolicies(60, 100));
            }
        } catch (Exception e) {
            log.warn("Namespace init skipped (may already exist): {}", e.getMessage());
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
