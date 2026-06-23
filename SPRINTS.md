# Plan de correction — Benchmark EDA Pulsar vs Artemis

> Généré le 2026-06-23 après revue multi-agents (88 défaillances identifiées sur 4 couches).  
> Mettre à jour ce fichier à chaque session : cocher les cases, noter les commits.

---

## Sprint 1 — Validité scientifique des mesures *(BLOQUANT — aucune conclusion défendable avant ce sprint)*

### 1.1 Unifier l'architecture de mesure Artemis/Pulsar
- [x] Réécrire `ArtemisBenchmarkClient` en mode concurrent : `sendAndRecord()` + `consumeAsync()` (FIFO position-based)
- [x] Supprimer le modèle stop-and-wait d'Artemis (`measureArtemis`, `sendAndMeasureBoth` en benchmark)
- [x] `t0` pris après `writeBytes()`, juste avant `producer.send()` (sérialisation exclue de la mesure)

### 1.2 Corriger le calcul du débit
- [x] Remplacer `wallTime(lats[0])` par `System.nanoTime()` encadrant la boucle complète pour Artemis
- [x] Unifier `benchmarkArtemis` et `streamArtemis` sur la même formule (`sendElapsed`)
- [x] Supprimer la méthode `wallTime` devenue inutile

### 1.3 Corriger les percentiles (p99/p99.9 surestimés)
- [x] Formule : `Math.min(n - 1, Math.max(0, (int) Math.ceil(n * pct) - 1))`
- [x] p99.9 est maintenant discriminant à partir de n = 1 000 (index 998/1000 au lieu du max)

### 1.4 Corriger la StdDev inter-runs (diviseur N au lieu de N-1)
- [x] `Math.sqrt(sum / (ms.length - 1))` avec guard `ms.length < 2 → 0` (correction de Bessel)

### 1.5 Corriger la mesure E2E
- [x] `recvNs[seq]` pris **avant** `acknowledge()` dans `consumeAsync` Pulsar
- [x] Supprimer `Math.max(recvNs[i] - sendNs[i], pub[i])` dans `computeE2e` ; log WARN si incohérence

### 1.6 Ajouter MB/s dans le sweep
- [x] `artemisThroughputMbSec` / `pulsarThroughputMbSec` ajoutés à `SweepPoint` et `SweepProgress`
- [x] Calculé comme `throughputMsgSec * payloadBytes / (1024² )` dans `BenchmarkService.sweepStreaming`
- [ ] Afficher dans le graphe Chart.js (second axe Y) — **différé Sprint 3**

**Commits :** `fix(bench): sprint1 — mesures iso Artemis/Pulsar, percentiles, stddev, MB/s sweep`  
**Statut : ✅ TERMINÉ**

---

## Sprint 2 — Infrastructure robuste *(prérequis pour des runs reproductibles)*

### 2.1 Mutex anti-concurrence sur les benchmarks
- [x] `AtomicBoolean benchmarkRunning` dans `BenchmarkService` → `IllegalStateException` si run actif
- [x] `AtomicBoolean featureRunning` dans `PulsarFeaturesService` → idem pour toutes les démos

### 2.2 Corriger les fuites de thread pools
- [x] `BenchmarkController` : `ExecutorService` singleton partagé + `@PreDestroy shutdown()`
- [x] `PulsarFeaturesController` : idem, threads nommés `pulsar-feature-N` daemon via `ThreadFactory`
- [x] Erreur mutex propagée comme event SSE `name("error")` au client (pas de 409 silencieux)

### 2.3 Isolation des topics Pulsar
- [x] Suffixe `UUID.randomUUID()` (12 chars hex) au lieu de `currentTimeMillis()` dans toutes les démos
- [x] `DLT` : UID partagé entre topic principal et topic DLT pour cohérence

### 2.4 Corriger le shutdown de `EmbeddedPulsarServer`
- [x] `try { pulsarService.close() } finally { bkEnsemble.stop() }` — BookKeeper/ZK toujours stoppés

### 2.5 Corriger les `close()` des clients (fuite si 1ère close() lève)
- [x] `ArtemisBenchmarkClient.close()` : chaîne `try-finally` (producer → producerSession → consumer → consumerSession → factory → locator)
- [x] `PulsarBenchmarkClient.close()` : `try-finally` (producer → consumer → client)

### 2.6 Nettoyage du journal Artemis (fuite tmpdir)
- [x] `journalDir` conservé comme champ de `EmbeddedArtemisServer`
- [x] `close()` : `Files.walk(journalDir).sorted(reverseOrder()).forEach(Files::delete)` après `server.stop()`

### 2.7 Corriger le TOCTOU des ports
- [ ] **Déféré** : risque faible en POC single-developer ; un retry au démarrage suffirait mais complexifie inutilement le code

### 2.8 Corriger la démo Replay (seek sur messages acquittés)
- [x] Remplacer `consumer.seek(MessageId.earliest)` par l'API `Reader` (lit directement depuis le ledger, indépendant de la rétention basée sur ACK)

### 2.9 Corriger la NPE Key_Shared (`Map.copyOf` + key null)
- [x] Guard `if (key == null) { acknowledge; continue; }` dans la boucle consumer Key_Shared

**Commits :** `fix(infra): sprint2 — mutex, thread pools, shutdown, UUID topics, Replay Reader, NPE`  
**Statut : ✅ TERMINÉ**

---

## Sprint 3 — IHM : affichage honnête et robuste

### 3.1 Remettre à zéro `bState` au lancement d'un nouveau run
- [x] `bState = {}` dans `resetBench()` (déjà présent) + `streamDone = false`
- [x] `syncComparatif()` : efface activement les cellules des brokers absents (innerHTML = `<span>—</span>`)

### 3.2 Afficher les paramètres du run dans le Comparatif
- [x] Ligne "Conditions du run" : `bParams` capturé au submit, affiché par `syncComparatif()`
- [x] Bandeau avertissement `#cmp-warn-params` si un seul broker mesuré

### 3.3 Ajouter p99.9 au tableau Comparatif
- [x] Lignes `cmp-a-p999 / cmp-p-p999`, `cmp-a-e99 / cmp-p-e99`, `cmp-a-e999 / cmp-p-e999`

### 3.4 Corriger la détection de fin de stream multi-runs
- [x] `boolean isFinalRun` ajouté à `BenchmarkProgress`
- [x] Tous les constructeurs dans `BenchmarkService` mis à jour (false/true selon position)
- [x] JS : `p.isFinalRun === true` remplace `p.p99StddevMs > 0`

### 3.5 Corriger les `TypeError` en sweep (`.toFixed()` sur `undefined`)
- [x] Guards `d.artemisP99Ms != null ? ... : null` avant tout accès
- [x] `null` passé aux datasets Chart.js pour points manquants
- [x] MB/s affiché dans le graphe sweep (second axe Y `y1`) + colonnes dans la table

### 3.6 Corriger le tooltip Chart.js (crash sur valeur `null`)
- [x] `c.parsed.y != null ? ... : null` dans tous les callbacks `label` (chart pub, e2e, sweep)

### 3.7 Corriger le badge d'égalité (▲ attribué à Pulsar si égaux)
- [x] `pWins = both && (m.lower ? m.kp < m.ka : m.kp > m.ka)` — false si stricte égalité

### 3.8 Corriger le faux "Erreur SSE" à la fermeture normale
- [x] `let streamDone = false` → `true` avant `activeEs.close()` sur dernier event
- [x] `onerror` : `if (streamDone) return;`
- [x] Event serveur renommé `bench-error` (controller) + `addEventListener('bench-error', ...)` côté JS
- [x] Idem pour sweep : `sweepDone` flag + `bench-error` listener
- [x] Fix cosmétique : `dlt-stats` `display:none` dupliqué supprimé

**Commits :** `fix(ihm): sprint3 — comparatif complet, isFinalRun, MB/s sweep, guards, streamDone`  
**Statut : ✅ TERMINÉ**

---

## Sprint 4 — Qualité et enrichissement méthodologique

### 4.1 Validation des paramètres (client + serveur)
- [x] Client JS : garde `messages` (100–50 000), `payloadSize` (0–65 536), `producerCount` (1–8), `runs` (1–5) — erreur affichée dans `errBox`
- [x] Serveur : `validateBenchParams()` dans `BenchmarkController` → `bench-error` SSE si hors bornes (doublon de sécurité)

### 4.2 Barre de progression multi-run fluide
- [x] `benchmarkArtemisWithProgress` / `benchmarkPulsarWithProgress` : partials `run`/`totalRuns` corrects pendant chaque run
- [x] `runStreamingMulti` : utilise ces variantes pour `producerCount = 1` ; mode parallèle inchangé
- [x] Single-run (`streamArtemis`/`streamPulsar`) : refactorisé pour utiliser les mêmes variantes

### 4.3 E2E partiels pendant le streaming
- [x] `consumeAsync(int n, long[] recvNs)` : overload avec tableau externe dans `ArtemisBenchmarkClient` et `PulsarBenchmarkClient`
- [x] `partialE2e(pub, sendNs, recvNs, sent)` : collecte les E2E des messages déjà reçus (recvNs[i] > 0)
- [x] `partial()` : corrigé pour gérer des tableaux E2E de taille ≠ `sent` (eLen-based clamp)
- [x] Approche sentinelle 0 : `recvNs` initialisé à zéro, 0 = pas encore reçu (suffisant pour ce POC)

### 4.4 Note de durabilité dans l'UI Comparatif
- [x] Section "Conditions de durabilité (ce POC)" dans le tableau Comparatif
- [x] Avertissement explicite "résultats valables en throughput relatif uniquement"

### 4.5 Option "mode séquentiel" documentée
- [x] N/A : stop-and-wait définitivement supprimé en Sprint 1 — architecture unifiée concurrent pour les deux brokers

**Commits :** `fix(bench): sprint4 — validation params, multi-run partials, E2E progressif, note durabilité`  
**Statut : ✅ TERMINÉ**

---

## Suivi global

| Sprint | Thème | Statut |
|--------|-------|--------|
| Sprint 1 | Validité des mesures | ✅ TERMINÉ |
| Sprint 2 | Infrastructure robuste | ✅ TERMINÉ |
| Sprint 3 | IHM honnête | ✅ TERMINÉ |
| Sprint 4 | Qualité & enrichissement | ✅ TERMINÉ |

---

## Référence — Défaillances identifiées par couche

| Couche | Nb défaillances | Critiques |
|--------|----------------|-----------|
| IHM (index.html) | 21 | 4 |
| Calcul & métriques (BenchmarkService) | 13 | 2 |
| Clients broker (Artemis/Pulsar clients) | 12 | 3 |
| Infrastructure & SSE | 29 | 9 |
| **Total** | **88** | **18** |
