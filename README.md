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
# Premier build (télécharge ~200 MB de dépendances Pulsar la première fois)
mvn package -DskipTests

# Lancer le runner
mvn -pl runner spring-boot:run
```

Pulsar met **5 à 15 secondes** à initialiser (ZooKeeper + BookKeeper + broker).
L'IHM est disponible dès que le log affiche `Pulsar embedded ready`.

```
http://localhost:8080/
```

---

## IHM

```
┌─ Initialisation ──────────────────────────────────────┐
│  [ Vérifier les brokers ]   Artemis — prêt   Pulsar — prêt │
├─ Paramètres du test de charge ────────────────────────┤
│  Warmup · Messages mesurés · Taille payload            │
│  Producteurs parallèles · Brokers à tester             │
│  [ Lancer le test ]  [ Annuler ]                       │
├─ Progression ─────────────────────────────────────────┤
│  Artemis ████████████░░░░  1 400 / 2 000               │
│  Pulsar  ░░░░░░░░░░░░░░░░  en attente…                 │
├─ Résultats ────────────────────────────────────────────┤
│          Artemis     Pulsar                            │
│  p50      X.XX ms    X.XX ms   ▲                       │
│  p99      X.XX ms    X.XX ms                           │
│  p99.9    X.XX ms    X.XX ms                           │
│  Débit    X XXX msg/s  X XXX msg/s  ▲                  │
│  [Bar chart Chart.js p50 / p99 / p99.9]                │
└───────────────────────────────────────────────────────┘
```

### Paramètres

| Paramètre | Défaut | Description |
|---|---|---|
| Warmup | 200 | Messages de chauffe (JIT, cache) — non mesurés |
| Messages mesurés | 2 000 | Fenêtre de mesure par broker |
| Taille payload | 0 | 0 = `ContratEvent` JSON (~150 B) ; sinon N octets aléatoires |
| Producteurs parallèles | 1 | Threads producteurs concurrents par broker |
| Brokers | Artemis + Pulsar | Cocher/décocher pour exclure un broker |

---

## Endpoints REST

| Méthode | URL | Description |
|---------|-----|-------------|
| `GET` | `/` | IHM Thymeleaf |
| `GET` | `/benchmark/health` | Probe les deux brokers (1 msg aller-retour) |
| `GET` | `/benchmark/stream` | Flux SSE en temps réel (paramètres ci-dessous) |
| `GET` | `/benchmark` | Run synchrone JSON (legacy, pour scripting) |

Paramètres de `/benchmark/stream` :

```
?warmup=200&messages=2000&payloadSize=0&artemis=true&pulsar=true&producerCount=1
```

### Exemple curl

```bash
# Run JSON complet (bloquant)
curl "http://localhost:8080/benchmark?warmup=100&messages=1000"

# Health check
curl http://localhost:8080/benchmark/health
```

---

## Structure du projet

```
poc-pulsar-artemis/
├── pom.xml                         # parent multi-module (Java 17, SB 3.3.5)
├── broker-artemis/                 # module Artemis embedded
│   └── …/artemis/
│       ├── EmbeddedArtemisServer   # démarre EmbeddedActiveMQ sur port libre
│       └── ArtemisBenchmarkClient  # prod/conso Core protocol, mode producerOnly
├── broker-pulsar/                  # module Pulsar embedded
│   └── …/pulsar/
│       ├── EmbeddedPulsarServer    # ZooKeeper + BookKeeper + PulsarService
│       └── PulsarBenchmarkClient   # prod/conso, mode producerOnly
└── runner/                         # module Spring Boot
    └── …/runner/
        ├── RunnerApplication       # démarre les deux serveurs comme beans Spring
        ├── BenchmarkParams         # record des paramètres (warmup, messages, …)
        ├── BenchmarkProgress       # record émis dans le flux SSE
        ├── BenchmarkResult         # record résultat final (legacy JSON)
        ├── BenchmarkService        # orchestration : single-thread et parallèle
        ├── BenchmarkController     # endpoints REST + SSE + Thymeleaf
        └── domain/ContratEvent     # événement métier assurance (SOUSCRIPTION, …)
```

---

## Versions

| Composant | Version |
|-----------|---------|
| Java | 17 |
| Spring Boot | 3.3.5 |
| ActiveMQ Artemis | 2.36.0 |
| Apache Pulsar | 3.3.5 |
| Jackson | 2.17.2 |

---

## Mesures et interprétation

- **Latence publish** : `nanoTime` avant/après `send()` **synchrone et bloquant** —
  inclut le round-trip réseau local (loopback) + ack broker.
- **Pas de batching** : chaque message est un round-trip indépendant.
- **Percentiles** calculés sur l'ensemble des latences triées après le run
  (pas de HDR Histogram).
- **Mode parallèle** (`producerCount > 1`) : N threads producteurs concurrents,
  chacun avec sa propre connexion. Latences fusionnées avant calcul des percentiles.
  Le débit affiché est le débit agrégé (`N × messages / temps total`).
- **Environnement** : les deux brokers partagent la même JVM et le même CPU —
  les chiffres mesurent le comportement embedded, pas un cluster de production.
