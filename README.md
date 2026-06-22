# EDA Benchmark — Assurance

Banc de mesure comparatif **Apache Pulsar / RabbitMQ / Apache ActiveMQ Artemis**
dans un contexte d'architecture événementielle assurance, plus un POC Spring Boot
illustrant un flux contrats via Pulsar avec schema registry.

L'objectif est de produire des **mesures honnêtes et reproductibles**, pas de
démontrer qu'un broker est « le plus rapide ». Lire [METHODOLOGY.md](METHODOLOGY.md)
avant toute interprétation des chiffres.

---

## Prérequis

| Outil | Version minimale |
|-------|-----------------|
| Docker | 24+ |
| Docker Compose | v2.20+ (`docker compose`, pas `docker-compose`) |
| Bash | 4+ (Git Bash sur Windows) |
| `jq` | 1.6+ (pour `collect-results.sh`) |
| RAM libre | 4 Go recommandés pour les 3 brokers simultanés |

**Aucun JDK local requis** : OMB et l'application Spring Boot s'exécutent dans des conteneurs.

---

## Quickstart — smoke benchmark en < 15 min

```bash
# 1. Cloner le dépôt
git clone <url-du-depot>
cd poc-pulsar-artemis

# 2. Copier l'environnement (les valeurs par défaut fonctionnent sans modification)
cp .env.example .env

# 3. Démarrer les trois brokers
docker compose --profile brokers up -d

# 4. Attendre que les trois brokers soient healthy (~60 s la première fois)
docker compose ps   # Vérifier que STATUS = healthy pour pulsar, rabbitmq, artemis

# 5. Démarrer le runner OMB (build ~5 min la première fois — télécharge OMB + Maven)
docker compose --profile bench up -d --build omb-runner

# 6. Lancer le smoke workload sur les trois brokers
./omb/run.sh workloads/smoke.yaml pulsar
./omb/run.sh workloads/smoke.yaml rabbitmq
./omb/run.sh workloads/smoke.yaml artemis

# 7. Afficher le résumé des résultats
./scripts/collect-results.sh
```

> La première exécution de l'étape 5 clone OMB depuis GitHub et build le projet
> (Maven ~5 min). Les runs suivants utilisent l'image déjà construite (<1 min).

---

## Workloads complets

### Workload A — Temps réel transactionnel

```bash
# Palier 500 msg/s
./omb/run.sh workloads/A-realtime-transactional.yaml pulsar
./omb/run.sh workloads/A-realtime-transactional.yaml rabbitmq
./omb/run.sh workloads/A-realtime-transactional.yaml artemis

# Modifier producerRate dans le YAML pour les paliers 750 et 1000 msg/s
# ou passer l'argument OMB directement :
docker exec omb-runner /omb/bin/benchmark \
  --drivers /omb/drivers/pulsar.yaml \
  --workloads /omb/workloads/A-realtime-transactional.yaml \
  --output /omb/results/pulsar-A-750.json \
  --producerRate 750
```

### Workload B — Rafale batch de règlement

```bash
./omb/run.sh workloads/B-batch-settlement-burst.yaml pulsar
./omb/run.sh workloads/B-batch-settlement-burst.yaml rabbitmq
./omb/run.sh workloads/B-batch-settlement-burst.yaml artemis
```

### Workload C — Échange inter-SI (gros payload 32 Ko)

```bash
./omb/run.sh workloads/C-intersi-large-payload.yaml pulsar
./omb/run.sh workloads/C-intersi-large-payload.yaml rabbitmq
./omb/run.sh workloads/C-intersi-large-payload.yaml artemis
```

### Capturer les stats ressources

Dans un terminal séparé, pendant un run :

```bash
./scripts/capture-stats.sh A-pulsar 5    # échantillon toutes les 5 s
# Ctrl-C pour arrêter → results/stats-A-pulsar-<ts>.log
```

---

## POC Spring Boot (coding kata)

```bash
# Démarrer Pulsar + l'application Spring Boot
docker compose --profile app up -d --build app

# Attendre que l'app soit démarrée (~30 s)
docker logs -f insurance-app

# Émettre un événement de souscription
curl -X POST http://localhost:8081/contrats/events \
  -H "Content-Type: application/json" \
  -d '{"contractId":"CTR-001","eventType":"SOUSCRIPTION","titulaire":"Jean Dupont","montant":50000}'

# Observer les logs du consommateur
docker logs -f insurance-app
```

**Scénario kata Key_Shared** : lancer deux instances de l'app, envoyer des événements
avec le même `contractId` — observer que les messages arrivent toujours sur la même instance.

```bash
# Deuxième instance sur le port 8082 (modifier Dockerfile/port si besoin)
# ou via docker compose scale
docker compose --profile app up -d --scale app=2
```

---

## Structure du dépôt

```
.
├── brokers/
│   ├── pulsar/standalone.conf      # fsync BookKeeper (journalSyncData=true)
│   ├── rabbitmq/                   # quorum queues + wal_sync_method=sync
│   └── artemis/broker.xml          # journal-sync-*=true + BLOCK
├── omb/
│   ├── Dockerfile                  # JDK 17 / Maven 3.9.9 / clone OMB master
│   ├── run.sh                      # script de lancement
│   ├── drivers/                    # configs driver pointant sur hostnames Compose
│   └── workloads/                  # smoke + A + B + C
├── app/                            # POC Spring Boot 4.1.0 / Java 25
├── scripts/
│   ├── capture-stats.sh            # docker stats pendant les runs
│   └── collect-results.sh          # tableau de synthèse des JSON OMB
├── results/                        # JSON OMB + stats (gitignorés sauf .gitkeep)
├── METHODOLOGY.md                  # protocole, caveats, matrice durabilité
└── docker-compose.yml              # profils : brokers / bench / app
```

---

## Arrêter l'environnement

```bash
# Arrêter tout (conserver les volumes)
docker compose --profile brokers --profile bench --profile app down

# Arrêter et supprimer les volumes (reset complet)
docker compose --profile brokers --profile bench --profile app down -v
```

---

## Points importants avant d'interpréter les chiffres

- **Iso-durabilité** : les trois brokers sont configurés avec fsync par message.
  Voir [METHODOLOGY.md §1](METHODOLOGY.md#1-matrice-iso-durabilité).
- **Limite driver Artemis** : 1 producteur / 1 consommateur par queue dans OMB.
  Les workloads sont normalisés autour de cette contrainte.
  Voir [METHODOLOGY.md §3](METHODOLOGY.md#3-limite-du-driver-artemis--topologie).
- **Mono-nœud** : la réplication BookKeeper de Pulsar (avantage HA) n'est pas mesurée.
  Voir [METHODOLOGY.md §5](METHODOLOGY.md#5-caveats).
