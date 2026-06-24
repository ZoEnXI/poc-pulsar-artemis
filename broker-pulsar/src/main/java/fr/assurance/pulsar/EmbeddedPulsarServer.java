package fr.assurance.pulsar;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.Set;

/**
 * Démarre un Pulsar standalone in-process :
 *   ZooKeeper embedded → BookKeeper embedded → PulsarService (broker binaire uniquement)
 *
 * Le web service HTTP (Jetty/Jersey) est délibérément désactivé pour éviter les
 * conflits de classpath (javax.ws.rs vs jakarta.ws.rs, Jetty 9 vs 12).
 * La création du tenant/namespace utilise l'API interne (MetadataStore/ZooKeeper)
 * plutôt que le client admin REST.
 */
public class EmbeddedPulsarServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedPulsarServer.class);

    private LocalBookkeeperEnsemble bkEnsemble;
    private PulsarService pulsarService;
    private int brokerPort;

    public void start() throws Exception {
        int zkPort  = freePort();
        int bkPort  = freePort();
        brokerPort  = freePort();

        // Répertoires fixes dans le temp système — purgés à chaque démarrage (clearOldData=true)
        // pour éviter l'accumulation de ledgers BK résiduels entre sessions.
        String tmpBase = System.getProperty("java.io.tmpdir") + File.separator + "poc-pulsar-embedded";
        log.info("Starting ZooKeeper + BookKeeper on zk={} bk={} dataDir={}", zkPort, bkPort, tmpBase);
        bkEnsemble = new LocalBookkeeperEnsemble(1, zkPort, bkPort,
                tmpBase + "-zk", tmpBase + "-bk", true);
        bkEnsemble.start();

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setClusterName("standalone");
        conf.setAdvertisedAddress("localhost");
        conf.setMetadataStoreUrl("zk:127.0.0.1:" + zkPort);
        conf.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + zkPort);
        conf.setBrokerServicePort(Optional.of(brokerPort));
        conf.setWebServicePort(Optional.of(freePort()));

        // Quorum mono-bookie
        conf.setManagedLedgerDefaultEnsembleSize(1);
        conf.setManagedLedgerDefaultWriteQuorum(1);
        conf.setManagedLedgerDefaultAckQuorum(1);

        // Désactiver les composants non nécessaires pour le POC
        conf.setFunctionsWorkerEnabled(false);
        conf.setTransactionCoordinatorEnabled(false);

        // Réduire la pression sur le bookie unique : limiter la fréquence des cursor-writes
        // (chaque consumer.acknowledge() déclenche un cursor-write async → saturation sans ça)
        conf.setManagedLedgerDefaultMarkDeleteRateLimit(1000.0);
        // Éviter les rollovers fréquents qui ré-allouent des ledgers BK
        conf.setManagedLedgerMaxEntriesPerLedger(200_000);
        // Désactiver le throttle côté client (le bookie embedded régule lui-même)
        conf.setBookkeeperClientThrottleValue(0);

        conf.setAllowAutoTopicCreation(true);
        conf.setAllowAutoTopicCreationType(
                org.apache.pulsar.common.policies.data.TopicType.PARTITIONED);
        conf.setDefaultNumPartitions(1);

        conf.setSuperUserRoles(Set.of("admin"));
        conf.setAuthenticationEnabled(false);
        conf.setAuthorizationEnabled(false);

        log.info("Starting Pulsar broker (binary only) on port {}", brokerPort);
        pulsarService = new PulsarService(conf);
        pulsarService.start();

        // Créer tenant + namespaces via l'API interne (pas de REST/HTTP)
        initNamespaceInternal();

        log.info("Pulsar embedded ready — brokerUrl={}", getBrokerUrl());
    }

    public String getBrokerUrl() {
        return "pulsar://localhost:" + brokerPort;
    }

    @Override
    public void close() throws Exception {
        try {
            if (pulsarService != null) pulsarService.close();
        } finally {
            // BookKeeper/ZooKeeper doivent être stoppés même si pulsarService.close() lève
            if (bkEnsemble != null) bkEnsemble.stop();
        }
        log.info("Pulsar embedded stopped");
    }

    /**
     * Crée tenant "public" + namespaces "public/default" et "public/demo"
     * directement dans le MetadataStore (ZooKeeper), sans passer par HTTP.
     */
    private void initNamespaceInternal() {
        try {
            var resources = pulsarService.getPulsarResources();
            var tenantRes = resources.getTenantResources();
            var nsRes     = resources.getNamespaceResources();

            if (!tenantRes.tenantExists("public")) {
                tenantRes.createTenant("public",
                        TenantInfo.builder()
                                  .allowedClusters(Set.of("standalone"))
                                  .build());
                log.info("Tenant 'public' created");
            }

            var defaultNs = NamespaceName.get("public", "default");
            if (!nsRes.namespaceExists(defaultNs)) {
                var p = new Policies();
                p.replication_clusters = Set.of("standalone");
                // Fix #2 : supprimer les messages dès que tous les cursors ont acquitté
                // (0, 0) = pas de retention par taille ni par temps → GC BK immédiat
                p.retention_policies = new RetentionPolicies(0, 0);
                // Filet de sécurité : expirer les messages après 5min même si la
                // subscription a disparu sans tout acquitter (evite les ledgers orphelins)
                p.message_ttl_in_seconds = 300;
                nsRes.createPolicies(defaultNs, p);
                log.info("Namespace 'public/default' created (retention=0, ttl=300s)");
            }

            var demoNs = NamespaceName.get("public", "demo");
            if (!nsRes.namespaceExists(demoNs)) {
                var demoPolicies = new Policies();
                demoPolicies.replication_clusters = Set.of("standalone");
                demoPolicies.retention_policies = new RetentionPolicies(60, 100);
                nsRes.createPolicies(demoNs, demoPolicies);
                log.info("Namespace 'public/demo' created");
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
