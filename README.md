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

## IHM — 3 onglets

### Onglet 1 — Benchmark

```
┌─ Initialisation ──────────────────────────────────────────────┐
│  [ Vérifier les brokers ]   Artemis — prêt   Pulsar — prêt    │
├─ Paramètres du test de charge ────────────────────────────────┤
│  Warmup · Messages mesurés · Taille payload (0–65 536 B)      │
│  Producteurs parallèles (1–8) · Runs (1–5) · Brokers          │
│  [ Lancer le test ]  [ Annuler ]                              │
├─ Progression (streaming SSE intra-run) ───────────────────────┤
│  Artemis ████████████░░░░  1 400 / 2 000  p99 : 0.42 ms       │
│  Pulsar  ████░░░░░░░░░░░░    600 / 2 000  p99 : 0.18 ms       │
├─ Résultats ───────────────────────────────────────────────────┤
│          Artemis              Pulsar                           │
│  p50      X.XX ms             X.XX ms   ▲                     │
│  p99      X.XX ± Y ms         X.XX ms                         │
│  p99.9    X.XX ms             X.XX ms                         │
│  Débit    X XXX msg/s         X XXX msg/s  ▲                  │
│  E2E p50  X.XX ms             X.XX ms                         │
│  E2E p99  X.XX ms             X.XX ms                         │
│  [Bar chart Chart.js : p50 / p99 / p99.9, pub + E2E]          │
├─ Payload Sweep (128 B → 64 KB) ───────────────────────────────┤
│  [Line chart : latence p99 (ms) axe gauche]                    │
│  [Line chart : débit MB/s axe droit, en pointillés]            │
│  [Table : Taille | Artemis p99 | Pulsar p99 | MB/s × 2]       │
└───────────────────────────────────────────────────────────────┘
```

### Onglet 2 — Fonctionnalités Pulsar

Quatre démos interactives SSE illustrant les fonctionnalités exclusives de Pulsar :

| Démo | Description |
|------|-------------|
| **Key_Shared** | N consumers partagent une subscription ; Pulsar garantit que chaque clé (ex. `contractId`) est toujours routée vers le même consumer via un anneau de hachage broker-side. |
| **Message Replay** | Messages relus depuis le début via l'API `Reader` après acquittement complet — rendu possible par la rétention configurable (ici 60 min / 100 MB sur `public/demo`). |
| **Dead Letter Topic** | Messages empoisonnés reroutés automatiquement vers un DLT après `maxRedeliverCount` tentatives — 0 perte, messages récupérables pour audit. |
| **Fan-out** | Deux subscriptions indépendantes sur le même topic reçoivent chacune l'intégralité du flux sans coordination ni duplication de données. |

### Onglet 3 — Comparatif

Tableau statique Artemis vs Pulsar (subscription types, persistence, replay, opérationnel)
enrichi dynamiquement avec les résultats du dernier benchmark :
- Conditions du run (messages / payload / producteurs / runs)
- Latences pub p50 / p99 / p99.9 et E2E
- Débit msg/s
- Note de durabilité : journal tmpdir + fsync désactivé dans ce POC → résultats relatifs uniquement
- Bandeau avertissement si les deux brokers n'ont pas été mesurés dans le même run

---

## Paramètres du benchmark

| Paramètre | Défaut | Bornes | Description |
|-----------|--------|--------|-------------|
| Warmup | 200 | 0–5 000 | Messages de chauffe (JIT, cache) — non mesurés |
| Messages mesurés | 2 000 | **100–50 000** | Fenêtre de mesure par broker |
| Taille payload | 0 | **0–65 536 B** | 0 = `ContratEvent` JSON (~150 B) ; N = N octets aléatoires |
| Producteurs parallèles | 1 | **1–8** | Threads producteurs concurrents (> 1 = pub-only, E2E désactivé) |
| Nombre de runs | 1 | **1–5** | Répète le test N fois, affiche moyenne ± écart-type (Bessel) |
| Brokers | A + P | — | Cocher/décocher pour exclure un broker |

Les bornes sont vérifiées côté client (erreur inline) **et** côté serveur (event `bench-error` SSE).

---

## Endpoints REST

| Méthode | URL | Description |
|---------|-----|-------------|
| `GET` | `/` | IHM Thymeleaf |
| `GET` | `/benchmark/health` | Probe les deux brokers (1 msg aller-retour) |
| `GET` | `/benchmark/durability` | Infos durabilité des deux brokers |
| `GET` | `/benchmark/stream` | Flux SSE benchmark (paramètres ci-dessous) |
| `GET` | `/benchmark/sweep/stream` | Sweep payload 128 B → 64 KB via SSE |
| `GET` | `/benchmark` | Run synchrone JSON (legacy, scripting) |
| `GET` | `/pulsar/key-shared/stream` | Démo Key_Shared SSE |
| `GET` | `/pulsar/replay/stream` | Démo Message Replay SSE |
| `GET` | `/pulsar/dlt/stream` | Démo Dead Letter Topic SSE |
| `GET` | `/pulsar/fanout/stream` | Démo Fan-out SSE |

Paramètres de `/benchmark/stream` :

```
?warmup=200&messages=2000&payloadSize=0&artemis=true&pulsar=true&producerCount=1&runs=1
```

### Exemple curl

```bash
# Health check
curl http://localhost:8080/benchmark/health

# Run JSON complet (bloquant)
curl "http://localhost:8080/benchmark?warmup=100&messages=1000"

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
├── pom.xml                              # unique POM (Spring Boot 3.3.5, Java 17)
├── SPRINTS.md                           # suivi des 4 sprints de correction
├── broker-artemis/src/main/java/…/artemis/
│   ├── EmbeddedArtemisServer            # démarre EmbeddedActiveMQ sur port libre
│   └── ArtemisBenchmarkClient           # prod/conso Core protocol, producerOnly
│       # consumeAsync(n, long[] recvNs) : tableau externe pour E2E progressif
├── broker-pulsar/src/main/java/…/pulsar/
│   ├── EmbeddedPulsarServer             # ZooKeeper + BookKeeper + PulsarService
│   └── PulsarBenchmarkClient            # prod/conso Pulsar, producerOnly
│       # consumeAsync(n, long[] recvNs) : idem
└── runner/src/main/
    ├── java/…/runner/
    │   ├── RunnerApplication            # démarre les deux serveurs comme beans Spring
    │   ├── BenchmarkParams              # record paramètres (warmup, messages, …)
    │   ├── BenchmarkProgress            # record SSE (isFinalRun, e2e*, p99StddevMs)
    │   ├── BenchmarkResult              # record résultat final (legacy JSON)
    │   ├── SweepPoint / SweepProgress   # records SSE sweep (artemis/pulsarThroughputMbSec)
    │   ├── BenchmarkService             # orchestration : single + multi-run + sweep
    │   │   # modèle concurrent : consumeAsync() démarre avant la boucle send
    │   │   # benchmarkArtemisWithProgress / benchmarkPulsarWithProgress : partials
    │   │   # partialE2e() : E2E progressif pendant le streaming
    │   ├── BenchmarkController          # REST + SSE + Thymeleaf + validateBenchParams()
    │   └── pulsar/
    │       ├── PulsarFeaturesController # 4 endpoints SSE démos Pulsar
    │       └── PulsarFeaturesService    # Key_Shared, Replay, DLT, Fan-out
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
| Chart.js | 4.4.4 |

> **Compatibilité Spring Boot / Pulsar :** Pulsar 4.2.2 nécessite plusieurs overrides
> de dépendances dans le `pom.xml` (Jetty 12.1.10, Jersey 2.42, OpenTelemetry 1.56,
> JAXB 2.3.3 EE8) pour cohabiter avec Spring Boot 3.3.5. Ces overrides sont documentés
> dans les commentaires du `pom.xml`.

---

## Architecture de mesure

### Modèle concurrent (identique pour Artemis et Pulsar)

```
Thread producer                Thread consumer (daemon)
──────────────────             ────────────────────────
consumeAsync(n, recvNs) ──────► démarre, écoute FIFO/seq
t0 = nanoTime()
for i in [0, n):
  sendNs[i], pub[i] = sendAndRecord(payload)
  if i % 100 == 0: émet partial
    ← recvNs[i] lu (best-effort, race intentionnelle)
sendElapsed = nanoTime() - t0
recvFuture.get(60s) ──────────► complète recvNs[]
e2e[i] = recvNs[i] - sendNs[i]
```

- **Artemis** : corrélation par position FIFO (`recvNs[position]`) — ordre garanti single-producer
- **Pulsar** : corrélation par seqno (`recvNs[parseInt(messageKey)]`) — clé = index d'envoi
- La phase de warmup (`drain()`) purge la queue avant la mesure

### Calcul des métriques

| Métrique | Formule |
|----------|---------|
| **Latence publish** | `nanoTime` avant/après `send()` synchrone et bloquant |
| **Latence E2E** | `recvNs[i] - sendNs[i]`, capturé **avant** `acknowledge()` |
| **Percentile p** | `sorted[ceil(n × p) - 1]` (formule inclusive) |
| **Débit** | `n / sendElapsed` (wall-clock de la boucle send, pas somme des latences) |
| **MB/s (sweep)** | `throughputMsgSec × payloadBytes / 1 048 576` |
| **Stddev inter-runs** | `√(Σ(xi - x̄)² / (N-1))` — correction de Bessel |
| **E2E partiel** | collecte `recvNs[i] > 0` pendant le streaming (race acceptable) |

### Limites connues

- Les deux brokers partagent la même JVM et le même CPU → chiffres relatifs, pas production
- Durabilité désactivée dans ce POC (journal/BK tmpdir, fsync off) → throughput ≠ config durable
- `producerCount > 1` désactive la mesure E2E (corrélation producer↔consumer impossible en parallèle)
- Pulsar : le premier run est plus lent (warm-up Netty + ledger BookKeeper)
