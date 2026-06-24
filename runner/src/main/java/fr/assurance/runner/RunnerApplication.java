package fr.assurance.runner;

import fr.assurance.artemis.EmbeddedArtemisServer;
import fr.assurance.pulsar.EmbeddedPulsarServer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
public class RunnerApplication {

    public static void main(String[] args) {
        SpringApplication.run(RunnerApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    @Profile("!external")
    EmbeddedArtemisServer artemisServer() throws Exception {
        EmbeddedArtemisServer server = new EmbeddedArtemisServer();
        server.start();
        return server;
    }

    @Bean(destroyMethod = "close")
    @Profile("!external")
    EmbeddedPulsarServer pulsarServer() throws Exception {
        EmbeddedPulsarServer server = new EmbeddedPulsarServer();
        server.start();
        return server;
    }

    @Bean
    @Profile("!external")
    BrokerProperties embeddedBrokerProperties(EmbeddedArtemisServer artemisServer,
                                               EmbeddedPulsarServer pulsarServer) {
        return new BrokerProperties(
                "embedded",
                artemisServer.getBrokerUrl(),
                pulsarServer.getBrokerUrl(),
                "",   // pas d'admin HTTP en embedded (namespace créé via API interne)
                artemisServer.isPersistent() ? "journal (tmpdir, fsync off)" : "in-memory",
                "BookKeeper (tmpdir, fsync off)");
    }

    @Bean
    @Profile("external")
    BrokerProperties externalBrokerProperties(
            @Value("${broker.artemis.url}") String artemisUrl,
            @Value("${broker.pulsar.url}") String pulsarUrl,
            @Value("${broker.pulsar.admin-url}") String pulsarAdminUrl,
            @Value("${broker.artemis.durability}") String artemisDurability,
            @Value("${broker.pulsar.durability}") String pulsarDurability) {
        return new BrokerProperties("external", artemisUrl, pulsarUrl, pulsarAdminUrl,
                artemisDurability, pulsarDurability);
    }
}
