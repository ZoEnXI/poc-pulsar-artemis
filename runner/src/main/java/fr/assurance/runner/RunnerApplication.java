package fr.assurance.runner;

import fr.assurance.artemis.EmbeddedArtemisServer;
import fr.assurance.pulsar.EmbeddedPulsarServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class RunnerApplication {

    private static final Logger log = LoggerFactory.getLogger(RunnerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(RunnerApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    EmbeddedArtemisServer artemisServer() throws Exception {
        EmbeddedArtemisServer server = new EmbeddedArtemisServer();
        server.start();
        return server;
    }

    @Bean(destroyMethod = "close")
    EmbeddedPulsarServer pulsarServer() throws Exception {
        EmbeddedPulsarServer server = new EmbeddedPulsarServer();
        server.start();
        return server;
    }
}
