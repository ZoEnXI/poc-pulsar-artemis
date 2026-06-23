# EDA Benchmark — Artemis vs Pulsar

Banc de mesure comparatif **Apache ActiveMQ Artemis / Apache Pulsar** dans un contexte
d'architecture événementielle assurance (CNP / LBP).

Les deux brokers démarrent **en-process** (embedded JVM, zéro Docker) grâce à
`EmbeddedActiveMQ` et `LocalBookkeeperEnsemble`. Un runner Spring Boot expose une IHM
Thymeleaf avec résultats en quasi-temps-réel via Server-Sent Events.

---

## Prérequis

| Outil | Version |
|-------|---------|
| JDK   | 17+     |
| Maven | 3.9+    |

Aucun Docker, aucun broker externe, aucun Node.js.

---

## Démarrage

```bash
# Premier build (télécharge ~300 MB de dépendances Pulsar la première fois)
mvn package -DskipTests

# Lancer l'application
mvn spring-boot:run
```

Pulsar met **10 à 20 secondes** à initialiser (ZooKeeper + BookKeeper + broker).
L'IHM est disponible dès que le log affiche `Pulsar embedded ready`.

```
http://localhost:8080/
```

> **Note :** le premier appel à `/benchmark/health` envoie un vrai message à chaque
> broker — comptez 30 à 60 s pour le premier round-trip Pulsar, normal ensuite.

---

## IHM

```
┌─ Initialisation ───────────────────────────────────────────┐
│  [ Vérifier les brokers ]   Artemis — prêt   Pulsar — prêt │
├─ Paramètres du test de charge ─────────────────────────────┤
│  Warmup · Messages mesurés · Taille payload                 │
│  Producteurs parallèles · Nombre de runs · Brokers          │
│  [ Lancer le test ]  [ Annuler ]                            │
├─ Progression ──────────────────────────────────────────────┤
│  Artemis ████████████░░░░  1 400 / 2 000                    │
│  Pulsar  ░░░░░░░░░░░░░░░░  en attente…                      │
├─ Résultats ────────────────────────────────────────────────┤
│          Artemis          Pulsar                            │
│  p50      X.XX ± Y ms     X.XX ± Y ms   ▲                  │
│  p99      X.XX ms         X.XX ms                           │
│  p99.9    X.XX ms         X.XX ms                           │
│  Débit    X XXX msg/s     X XXX msg/s   ▲                   │
│  [Bar chart Chart.js p50 / p99 / p99.9]                     │
├─ Sweep taille de payload ──────────────────────────────────┤
│  [Line chart latence p50/p99 vs 128B … 64 KB]               │
└────────────────────────────────────────────────────────────┘
```

### Paramètres

| Paramètre | Défaut | Description |
|---|---|---|
| Warmup | 200 | Messages de chauffe (JIT, cache) — non mesurés |
| Messages mesurés | 2 000 | Fenêtre de mesure par broker |
| Taille payload | 0 | 0 = `ContratEvent` JSON (~150 B) ; sinon N octets aléatoires |
| Producteurs parallèles | 1 | Threads producteurs concurrents par broker |
| Nombre de runs | 1 | Répète le test 1 à 5 fois, affiche moyenne ± écart-type |
| Brokers | Artemis + Pulsar | Cocher/décocher pour exclure un broker |

---

## Endpoints REST

| Méthode | URL | Description |
|---------|-----|-------------|
| `GET` | `/` | IHM Thymeleaf |
| `GET` | `/benchmark/health` | Probe les deux brokers (1 msg aller-retour) |
| `GET` | `/benchmark/durability` | Infos durabilité des deux brokers |
| `GET` | `/benchmark/stream` | Flux SSE en temps réel (paramètres ci-dessous) |
| `GET` | `/benchmark/sweep/stream` | Sweep taille payload 128 B → 64 KB via SSE |
| `GET` | `/benchmark` | Run synchrone JSON (legacy, pour scripting) |

Paramètres de `/benchmark/stream` :

```
?warmup=200&messages=2000&payloadSize=0&artemis=true&pulsar=true&producerCount=1&runs=1
```

### Exemple curl

```bash
# Run JSON complet (bloquant)
curl "http://localhost:8080/benchmark?warmup=100&messages=1000"

# Health check
curl http://localhost:8080/benchmark/health

# Sweep payload (SSE)
curl -N "http://localhost:8080/benchmark/sweep/stream?warmup=50&messages=500"
```

---

## Structure du projet

Le projet est un **mono-module Maven**. Les sources de `broker-artemis/`, `broker-pulsar/`
et `runner/` sont agrégées via `build-helper-maven-plugin` en un seul cycle de
compilation/packaging.

```
poc-pulsar-artemis/
├── pom.xml                         # unique POM (Spring Boot 3.3.5, Java 17)
├── broker-artemis/src/main/java/
│   └── …/artemis/
│       ├── EmbeddedArtemisServer   # démarre EmbeddedActiveMQ sur port libre
│       └── ArtemisBenchmarkClient  # prod/conso Core protocol, mode producerOnly
├── broker-pulsar/src/main/java/
│   └── …/pulsar/
│       ├── EmbeddedPulsarServer    # ZooKeeper + BookKeeper + PulsarService
│       └── PulsarBenchmarkClient   # prod/conso binaire Pulsar, mode producerOnly
└── runner/src/main/
    ├── java/…/runner/
    │   ├── RunnerApplication       # démarre les deux serveurs comme beans Spring
    │   ├── BenchmarkParams         # record des paramètres (warmup, messages, …)
    │   ├── BenchmarkProgress       # record émis dans le flux SSE
    │   ├── BenchmarkResult         # record résultat final (legacy JSON)
    │   ├── BenchmarkService        # orchestration : single + multi-run + sweep
    │   ├── BenchmarkController     # endpoints REST + SSE + Thymeleaf
    │   └── domain/ContratEvent     # événement métier assurance (SOUSCRIPTION, …)
    └── resources/templates/index.html
```

---

## Versions

| Composant | Version |
|-----------|---------|
| Java | 17 |
| Spring Boot | 3.3.5 |
| ActiveMQ Artemis | 2.36.0 |
| Apache Pulsar | 4.2.2 |

> **Compatibilité Spring Boot / Pulsar :** Pulsar 4.2.2 nécessite plusieurs overrides
> de dépendances dans le `pom.xml` (Jetty 12.1.10, Jersey 2.42, OpenTelemetry 1.56,
> JAXB 2.3.3 EE8) pour cohabiter avec Spring Boot 3.3.5. Ces overrides sont documentés
> dans les commentaires du `pom.xml`.

---

## Mesures et interprétation

- **Latence publish** : `nanoTime` avant/après `send()` **synchrone et bloquant** —
  inclut le round-trip réseau local (loopback) + ack broker.
- **Latence E2E** : horodatage producteur → horodatage réception consommateur.
- **Pas de batching** : chaque message est un round-trip indépendant.
- **Percentiles** calculés sur l'ensemble des latences triées après le run
  (pas de HDR Histogram).
- **Multi-run** (`runs > 1`) : répète N fois le scénario, calcule moyenne ± écart-type
  sur les p50/p99/p99.9 pour évaluer la stabilité.
- **Mode parallèle** (`producerCount > 1`) : N threads producteurs concurrents,
  chacun avec sa propre connexion. Latences fusionnées avant calcul des percentiles.
  Le débit affiché est le débit agrégé.
- **Environnement** : les deux brokers partagent la même JVM et le même CPU —
  les chiffres mesurent le comportement embedded, pas un cluster de production.
