# EDA Benchmark — Artemis vs Pulsar

Banc de mesure comparatif **Apache ActiveMQ Artemis / Apache Pulsar** pour un contexte
événementiel assurance. Spring Boot + SSE, deux modes de déploiement.

---

## Démarrage rapide

### Mode embedded (défaut — zéro dépendance externe)

```bash
mvn package -DskipTests   # premier build : ~300 MB de dépendances Pulsar
mvn spring-boot:run
```

Les deux brokers démarrent **dans la même JVM**. Pulsar prend 10–20 s (ZooKeeper + BookKeeper).
L'IHM est disponible sur `http://localhost:8080` dès le log `Pulsar embedded ready`.

### Mode external (Docker — brokers réels)

```bash
docker compose up -d          # démarre Artemis 2.36.0 + Pulsar 4.2.2
mvn spring-boot:run -Dspring-boot.run.profiles=external
```

Les URLs sont dans `runner/src/main/resources/application-external.yml` :

```yaml
broker:
  artemis:
    url: tcp://localhost:61616
  pulsar:
    url: pulsar://localhost:6650
    admin-url: http://localhost:8081   # port 8080 du container mappé sur 8081
```

> L'admin HTTP (port 8081) est utilisé au démarrage pour créer le namespace `public/demo`
> requis par les démos Pulsar (Key_Shared, Replay, DLT, Fan-out).

---

## Prérequis

| Outil | Version | Mode |
|-------|---------|------|
| JDK | 17+ | les deux |
| Maven | 3.9+ | les deux |
| Docker Desktop | 4.x+ | external uniquement |

---

## Subtilités du POM

Le projet est un **mono-module Maven** — pas de multi-module parent/enfant. Les sources de
`broker-artemis/`, `broker-pulsar/` et `runner/` sont agrégées via `build-helper-maven-plugin`
dans un seul `pom.xml`. Avantage : `spring-boot:run` résout le classpath en un seul cycle,
sans `install` préalable des sous-modules.

### Overrides de dépendances obligatoires

Pulsar 4.2.2 embarque Jetty 9 et Jersey 2 (Jakarta EE 8). Spring Boot 3.3.5 attend
Jetty 12 et Jakarta EE 10. Sans les overrides suivants, le build produit des conflits
de classpath (double `javax.ws.rs` / `jakarta.ws.rs`) qui crashent au démarrage :

```xml
<!-- Forcer Jetty 12 (Spring Boot 3.x) -->
<jetty.version>12.0.14</jetty.version>

<!-- Forcer Jersey 2.x compatible Jakarta EE 8 (Pulsar) -->
<jersey.version>2.42</jersey.version>

<!-- OpenTelemetry aligné sur Pulsar 4.2.2 -->
<opentelemetry.version>1.44.1</opentelemetry.version>

<!-- JAXB EE8 pour Pulsar (Spring Boot exclut JAXB par défaut) -->
<dependency>
    <groupId>com.sun.xml.bind</groupId>
    <artifactId>jaxb-impl</artifactId>
    <version>2.3.3</version>
</dependency>
```

Le web service HTTP de Pulsar embedded (Jetty/Jersey) est **désactivé** pour éviter les
conflits restants. La création du tenant/namespace `public/demo` passe par l'API interne
MetadataStore (ZooKeeper) au lieu du REST admin.

---

## Embedded vs External — différences clés

| | Embedded | External |
|---|---|---|
| Brokers | In-process, même JVM | Docker, processus séparés |
| Démarrage | `mvn spring-boot:run` | `docker compose up -d` puis `mvn spring-boot:run -Dspring-boot.run.profiles=external` |
| Durabilité | Journal tmpdir, fsync off | Config Docker (journal/BK réels) |
| CPU Pulsar | Partagé avec l'app | Isolé dans le container |
| `public/demo` | Créé via API interne ZK | Créé via admin HTTP au démarrage |
| Résultats benchmark | Relatifs (JVM partagée) | Plus représentatifs |

---

## Limites connues

- **Embedded** : les deux brokers partagent CPU et heap avec Spring Boot → latences relatives uniquement
- **Pulsar** : `maxPendingMessages(1)` mesure la latence d'un message unique (pas le débit pipeliné) — intentionnel pour une comparaison équitable avec Artemis
- `producerCount > 1` désactive la mesure E2E (corrélation send↔recv impossible en parallèle)
- Le sweep 128 B → 64 KB ajoute 1 s entre chaque taille (GC BookKeeper embedded)
