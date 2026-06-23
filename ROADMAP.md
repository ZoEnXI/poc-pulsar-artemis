# Roadmap — Benchmark équitable & tests complémentaires

## Pourquoi les résultats embedded sont biaisés

En mode embedded (défaut), Artemis tourne **in-process** (0 socket, 0 I/O disque), tandis que
Pulsar passe par **TCP loopback + BookKeeper journal** même en POC.  
Artemis > Pulsar dans ce contexte reflète une asymétrie architecturale, pas une différence
de performance production. Les 3 phases ci-dessous y remédient progressivement.

---

## Phase 1 — Environnement équivalent (brokers externes)

**Objectif :** les deux brokers tournent comme processus Docker indépendants.
Même overhead TCP pour les deux → comparaison défendable.

### Démarrage

```bash
# Lancer les brokers (depuis la racine du projet)
docker compose up -d

# Lancer le runner en mode external
mvn spring-boot:run -Dspring-boot.run.profiles=external

# Mode embedded classique (inchangé)
mvn spring-boot:run
```

L'IHM affiche un badge **"Mode : External (Docker)"** (vert) ou **"Mode : Embedded (même JVM)"** (orange).

### Checklist Phase 1

- [x] `docker-compose.yml` — Artemis 2.36.0 + Pulsar 4.0.0 standalone
- [x] `BrokerProperties.java` — record découplant les URLs des serveurs embedded
- [x] `RunnerApplication.java` — beans conditionnels `@Profile("!external")` / `@Profile("external")`
- [x] `BenchmarkService.java` + `PulsarFeaturesService.java` — prennent `BrokerProperties`
- [x] `application-external.properties` — URLs Docker
- [x] `index.html` — badge mode + JS `loadMode()`
- [ ] Test mode external : vérifier benchmark complet contre Docker Artemis + Pulsar
- [ ] Comparer chiffres : Pulsar E2E attendu < 5 ms (vs 22 ms embedded)

---

## Phase 2 — Tests complémentaires

Chaque test est indépendant. Ils révèlent des dimensions que le benchmark actuel ne mesure pas.

### 2.1 Fan-out N subscribers

**Objectif :** montrer que Pulsar supporte N subscriptions sans duplication de données ;
Artemis nécessiterait N queues séparées.  
**Endpoint :** `GET /benchmark/fanout-scale/stream?messages=1000&maxSubscribers=10`  
**IHM :** line chart lag par subscriber count

- [ ] `FanoutScaleService` — itère [1, 2, 5, 10] subscribers, mesure lag du plus lent
- [ ] Artemis : affiche "N/A — requires N separate queues"
- [ ] Section IHM dans onglet Benchmark ou nouveau sous-onglet

### 2.2 Replay à volume

**Objectif :** mesurer débit de relecture sur 10K / 100K / 500K messages.  
**Endpoint :** `GET /benchmark/replay-scale/stream?messages=100000`  
**IHM :** tableau Pulsar (débit replay) | Artemis (N/A — messages détruits après ACK)

- [ ] Étend `demoReplay` avec volumes paramétrables
- [ ] Metric : msgs/s en relecture + temps total

### 2.3 Durabilité réelle (fsync activé)

**Objectif :** comparer latences quand la durabilité est activée sur les deux brokers.

- [ ] `EmbeddedArtemisServer` : exposer flag `fsync` (`journal-sync-transactional=true`)
- [ ] `EmbeddedPulsarServer` : exposer `journalSyncEnabled=true` via `ServiceConfiguration`
- [ ] Profil `durable` → `application-durable.properties`
- [ ] IHM : checkbox "Durabilité activée" avant de lancer

### 2.4 Charge soutenue

**Objectif :** détecter une dégradation des brokers sous charge prolongée (GC, buffer saturation).  
**Endpoint :** `GET /benchmark/sustained/stream?messages=500000&reportInterval=10000`  
**IHM :** line chart débit par fenêtre de 10 000 messages

- [ ] Metric : throughput par fenêtre temporelle
- [ ] Affiche si dégradation linéaire, plateau, ou effondrement

---

## Phase 3 — OpenMessaging Benchmark (référence industrie)

**Objectif :** chiffres standardisés comparables aux publications publiques des vendors.  
**Scope :** hors POC — nécessite VMs dédiées (≥ 8 Go RAM par broker, réseau isolé).

```bash
git clone https://github.com/openmessaging/benchmark
cd benchmark
mvn install -DskipTests
# Driver Pulsar
./bin/benchmark --drivers driver-pulsar/pulsar.yaml \
                workloads/1-topic-1-partition-1kb.yaml
# Driver Artemis (si disponible dans la version OMB utilisée)
./bin/benchmark --drivers driver-artemis/artemis.yaml \
                workloads/1-topic-1-partition-1kb.yaml
```

Output : JSON → Grafana ou import dans l'IHM (à définir).

- [ ] Provisionner 2 VMs dédiées (broker + client séparés)
- [ ] Tester les workloads latency-throughput curve
- [ ] Importer résultats dans l'IHM ou un dashboard Grafana

---

## Suivi global

| Phase | Description | Statut |
|-------|-------------|--------|
| Phase 1 | Environnement équivalent (Docker) | 🔄 Code livré — test Docker en attente |
| Phase 2.1 | Fan-out N subscribers | ⬜ À faire |
| Phase 2.2 | Replay volume | ⬜ À faire |
| Phase 2.3 | Durabilité réelle | ⬜ À faire |
| Phase 2.4 | Charge soutenue | ⬜ À faire |
| Phase 3 | OpenMessaging Benchmark | ⬜ À faire |
