# Méthodologie du benchmark

Ce document décrit le protocole de mesure, les réglages de durabilité, les caveats
et les règles de lecture des résultats. Il doit être lu **avant** d'interpréter
tout chiffre issu de ce dépôt.

---

## 1. Matrice iso-durabilité

L'objectif est de comparer les trois brokers **à durabilité disque équivalente**.
Un seul nœud par broker → la normalisation porte uniquement sur la durabilité disque (fsync).
La réplication / haute-disponibilité est un avantage **qualitatif** de Pulsar
(Ensembles BookKeeper), hors périmètre de ce benchmark mono-machine.

| Broker | Réglage clé | Valeur | Garantie apportée |
|--------|-------------|--------|-------------------|
| **Pulsar 3.0.17** | `journalSyncData` | `true` | fsync du journal BookKeeper à chaque écriture avant ack producteur |
| Pulsar | `managedLedgerDefaultWriteQuorum` | `1` | Mono-bookie (pas de réplication ; choisir > 1 en multi-nœud) |
| Pulsar | `managedLedgerDefaultAckQuorum` | `1` | L'ack producteur attend la confirmation du seul bookie |
| **RabbitMQ 4.0.9** | `x-queue-type` (policy) | `quorum` | Queue Raft — journal fsynced par Ra |
| RabbitMQ | `wal_sync_method` (advanced.config) | `sync` | Chaque entrée du WAL Raft est fsync'd avant ack |
| RabbitMQ | `delivery_mode` (driver OMB) | `2` (persistent) | Messages stockés sur disque avant ack |
| RabbitMQ | `publisherConfirms` (driver OMB) | `true` | Producteur bloqué jusqu'au confirm broker |
| **Artemis 2.36.0** | `persistence-enabled` | `true` | Toutes les files sont durables |
| Artemis | `journal-type` | `ASYNCIO` (fallback `NIO`) | Écriture via libaio (O_DSYNC) ou java.nio.FileChannel.force(true) |
| Artemis | `journal-sync-transactional` | `true` | fsync après chaque transaction committée |
| Artemis | `journal-sync-non-transactional` | `true` | fsync après chaque message hors transactionnel |
| Artemis | `address-full-policy` | `BLOCK` | Producteur bloqué (back-pressure réel) quand adresse pleine |

---

## 2. Tableau des versions

| Composant | Version | Rôle |
|-----------|---------|------|
| OMB | `master` (Apr 2026) | Framework de benchmark (JDK 17 / Maven 3.9.9) |
| `pulsar-client-all` (OMB) | `2.11.0` | Client Pulsar du driver OMB (`driver-pulsar/pom.xml`) |
| `amqp-client` (OMB) | `5.18.0` | Client RabbitMQ du driver OMB (`driver-rabbitmq/pom.xml`) |
| `artemis-core-client` (OMB) | `2.23.1` | Client Artemis du driver OMB (`driver-artemis/pom.xml`) |
| `apachepulsar/pulsar` | `3.0.17` | Broker Pulsar (3.0 LTS) |
| `rabbitmq` | `4.0.9-management` | Broker RabbitMQ avec UI management |
| `apache/activemq-artemis` | `2.36.0` | Broker Artemis |
| Spring Boot | `4.1.0` | POC applicatif |
| Spring Pulsar | `2.0.6` | Intégration Pulsar pour Spring Boot 4 |
| `pulsar-client` (Spring) | `4.2.2` | Client Pulsar embarqué par spring-pulsar 2.0.6 |
| Java (OMB runner) | `17` (eclipse-temurin) | Requis par OMB |
| Java (app) | `25` (eclipse-temurin) | Spring Boot 4.1.0 minimum Java 25 |

### Note de compatibilité clients ↔ brokers

**Pulsar** : Le client OMB `2.11.0` se connecte au broker `3.0.17`.
Pulsar maintient la rétro-compatibilité du protocole binaire (les serveurs acceptent
les clients des versions antérieures dans la fenêtre N-1 majeure).
Les opérations pub/sub de base sont intégralement couvertes.
Le client Spring Pulsar `4.2.2` contre broker `3.0.17` : le client négocie
le numéro de version de protocole au handshake ; les fonctionnalités avancées
non disponibles en `3.0.17` ne sont pas sollicitées (pub/sub JSON schema, Key_Shared).

**RabbitMQ** : `amqp-client 5.18.0` utilise le protocole AMQP 0-9-1, inchangé
depuis RabbitMQ 2.x. Compatible avec RabbitMQ 4.0.9.

**Artemis** : `artemis-core-client 2.23.1` contre broker `2.36.0`.
Le protocole Core Artemis est stable au sein de la série 2.x.
Les messages et les primitives de session utilisés par OMB n'ont pas changé
entre 2.23 et 2.36.

---

## 3. Limite du driver Artemis — topologie

Le driver `io.openmessaging.benchmark.driver.artemis.ArtemisDriver` **ne supporte
qu'un seul producteur et un seul consommateur par queue** dans OMB.

Les drivers Pulsar et RabbitMQ acceptent `producersPerTopic > 1` et
`consumersPerSubscription > 1`.

**Impact sur la comparabilité** :

- Les workloads sont conçus autour de la contrainte `1P / 1C` pour conserver
  une topologie identique entre les trois brokers.
- Les résultats d'Artemis reflètent donc un scénario single-thread producteur /
  single-thread consommateur, là où Pulsar et RabbitMQ pourraient être parallélisés.
- Il est **incorrect** de conclure qu'Artemis est plus lent : la limite est
  architecturale dans le driver OMB, pas dans le broker.
- Pour un test de débit maximal d'Artemis, il faudrait un outil dédié
  (artemis-perf intégré au broker) ou un driver OMB corrigé.

---

## 4. Gestion de l'omission coordonnée

L'omission coordonnée est un biais de mesure classique des tests de latence :
si le producteur se bloque en attendant une réponse, le timer s'arrête et
les latences futures ne sont pas comptées.

OMB gère ce problème nativement via son modèle **target-rate** :

- Le producteur vise un débit cible (`producerRate` msg/s) avec un scheduler
  indépendant.
- Si l'envoi prend plus longtemps que prévu, le prochain envoi est programmé
  avec le délai **original** (pas repoussé), ce qui maintient la pression réelle.
- Les latences sont mesurées de bout en bout (envoi → réception consommateur)
  via **HdrHistogram**, qui capture les percentiles exacts sans troncature.

Ce dépôt s'appuie sur ce mécanisme tel quel et **ne le réimplémente pas**.

---

## 5. Caveats

### 5.1 Mono-nœud — pas de parité de réplication

Ce benchmark tourne sur un seul nœud par broker.

- **Pulsar** en production supporte des Ensembles BookKeeper multi-nœuds
  (`ensemble=3, writeQuorum=3, ackQuorum=2`), offrant à la fois durabilité
  et disponibilité sans double écriture logicielle. C'est un avantage
  architectural non mesurable en mono-machine.
- **RabbitMQ** quorum queues nécessitent au minimum 3 nœuds pour `quorum-size=2`.
  En mono-nœud, `x-quorum-initial-group-size=1` fonctionne mais n'offre pas
  la tolérance aux pannes d'un quorum réel.
- **Artemis** : pas de réplication dans la configuration benchmark.

La normalisation porte **uniquement sur le fsync disque**. Les chiffres
reflètent la durabilité, pas la résilience. Ne pas extrapoler à un déploiement
haute-disponibilité sans reconfiguration.

### 5.2 Profils synthétiques

Les payloads sont générés aléatoirement par OMB (`messageSize` en bytes).
Ils reproduisent la **taille et le flux** des événements assurance, pas leur
contenu réel. Aucune donnée CNP ou LBP n'est utilisée.

### 5.3 Impact de la durabilité sur les chiffres bruts

À niveau de durabilité **identique** (fsync par message), les chiffres reflètent
l'efficacité d'implémentation du chemin d'écriture de chaque broker.

Si un broker est configuré sans durabilité (pas de fsync, messages en mémoire
seulement), ses chiffres seront mécaniquement meilleurs sur un seul nœud.
Ce n'est pas le cas ici — **toute comparaison à chiffres publiés par un
fournisseur doit vérifier le niveau de durabilité retenu dans le test**.

### 5.4 Géo-réplication et rétention long-terme (Pulsar)

La géo-réplication native de Pulsar et le tiered-storage (archivage S3/GCS)
sont des différenciateurs architecturaux non mesurables en mono-machine locale.
Ces fonctionnalités sont documentées comme avantages qualitatifs dans la
présentation de l'étude, hors scope du benchmark quantitatif.

---

## 6. Comment lire les résultats

Les JSON dans `results/` contiennent (entre autres) :

| Champ | Signification |
|-------|---------------|
| `publishRate` | Débit publish réel atteint (msg/s) |
| `publishLatency.quantiles["0.5"]` | Latence publish médiane (ms) |
| `publishLatency.quantiles["0.99"]` | Latence publish p99 (ms) |
| `publishLatency.quantiles["0.999"]` | Latence publish p99.9 (ms) |
| `endToEndLatency.quantiles["0.99"]` | Latence E2E (envoi → consommateur) p99 (ms) |
| `backlogSize` | Messages en attente côté consommateur |

**Règle d'interprétation** :

1. Comparer **à durabilité égale** (toutes les configs de ce dépôt le sont).
2. Un écart de latence à faible débit → overhead du chemin d'écriture (journal vs WAL).
3. Un écart de débit maximal → capacité du pipeline producteur-broker-bookie.
4. La montée du backlog indique la saturation : au-delà, le débit est limité
   par le consommateur, pas le broker.
5. Les percentiles élevés (p99.9) sont le signal d'alarme pour les SLAs temps réel.

Utiliser `scripts/collect-results.sh` pour un tableau de synthèse rapide.
