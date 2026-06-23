# Plan de correction — Benchmark EDA Pulsar vs Artemis

> Généré le 2026-06-23 après revue multi-agents (88 défaillances identifiées sur 4 couches).  
> Mettre à jour ce fichier à chaque session : cocher les cases, noter les commits.

---

## Sprint 1 — Validité scientifique des mesures *(BLOQUANT — aucune conclusion défendable avant ce sprint)*

### 1.1 Unifier l'architecture de mesure Artemis/Pulsar
- [ ] Réécrire `ArtemisBenchmarkClient` en mode concurrent (producer libre + consumer parallèle)
- [ ] Supprimer le modèle stop-and-wait d'Artemis
- [ ] `t0` pris après création du message, juste avant l'appel réseau (hors sérialisation)

### 1.2 Corriger le calcul du débit
- [ ] Remplacer `wallTime(lats[0])` par `System.nanoTime()` encadrant la boucle complète pour Artemis
- [ ] Unifier `benchmarkArtemis` et `streamArtemis` sur la même formule

### 1.3 Corriger les percentiles (p99/p99.9 surestimés)
- [ ] Formule : `Math.min(n - 1, (int) Math.ceil(n * pct) - 1)`
- [ ] Vérifier que p99.9 est discriminant à partir de n = 1 000

### 1.4 Corriger la StdDev inter-runs (diviseur N au lieu de N-1)
- [ ] `Math.sqrt(sum / (ms.length - 1))` avec guard `ms.length < 2 → 0`

### 1.5 Corriger la mesure E2E
- [ ] `recvNs[seq]` pris **avant** `acknowledge()` dans `consumeAsync` Pulsar (aligner sur Artemis)
- [ ] Supprimer `Math.max(recvNs[i] - sendNs[i], pub[i])` ; logguer en WARN si incohérence

### 1.6 Ajouter MB/s dans le sweep
- [ ] Calculer `throughputMbSec` dans `SweepPoint`
- [ ] Exposer dans l'API SSE et afficher dans le graphe Chart.js (second axe Y ou dataset dédié)

**Commits :** à venir  
**Statut : 🔄 EN COURS**

---

## Sprint 2 — Infrastructure robuste *(prérequis pour des runs reproductibles)*

### 2.1 Mutex anti-concurrence sur les benchmarks
- [ ] `AtomicBoolean running` dans `BenchmarkService` → HTTP 409 si run actif
- [ ] Même guard dans `PulsarFeaturesService`

### 2.2 Corriger les fuites de thread pools
- [ ] `BenchmarkController` : un seul `ExecutorService` singleton + `@PreDestroy shutdown()`
- [ ] `PulsarFeaturesController` : idem, threads nommés via `ThreadFactory`

### 2.3 Isolation des topics Pulsar
- [ ] Suffixe `UUID.randomUUID()` au lieu de `currentTimeMillis()` dans les démos

### 2.4 Corriger le shutdown de `EmbeddedPulsarServer`
- [ ] Triple `try-finally` : `pulsarService.close()` → `bkEnsemble.stop()` (toujours exécuté)

### 2.5 Corriger les `close()` des clients (fuite si 1ère close() lève)
- [ ] `ArtemisBenchmarkClient.close()` : try-finally chaîné
- [ ] `PulsarBenchmarkClient.close()` : idem

### 2.6 Nettoyage du journal Artemis (fuite tmpdir)
- [ ] `EmbeddedArtemisServer.close()` : `Files.walk(journalDir)` + delete récursif

### 2.7 Corriger le TOCTOU des ports
- [ ] Artemis : conserver le `ServerSocket` ouvert jusqu'au `addAcceptorConfiguration`
- [ ] Pulsar : idem pour les 3 ports ZK/BK/broker

### 2.8 Corriger la démo Replay (seek sur messages acquittés)
- [ ] Utiliser l'API `Reader` Pulsar au lieu de `seek(earliest)` sur consumer avec messages acquittés

### 2.9 Corriger la NPE Key_Shared (`Map.copyOf` + key null)
- [ ] Guard `if (key != null)` avant `assignments.put(key, cid)`

**Commits :** à venir  
**Statut : 🔄 EN COURS**

---

## Sprint 3 — IHM : affichage honnête et robuste

### 3.1 Remettre à zéro `bState` au lancement d'un nouveau run
- [ ] `bState[broker] = null` au clic "Lancer", avant ouverture SSE
- [ ] `syncComparatif()` n'affiche rien si `bState` vide pour un broker

### 3.2 Afficher les paramètres du run dans le Comparatif
- [ ] Ligne d'en-tête : Messages / Payload / Producteurs / Runs pour chaque colonne
- [ ] Avertissement si paramètres différents entre Artemis et Pulsar

### 3.3 Ajouter p99.9 au tableau Comparatif
- [ ] Aligner avec l'onglet Benchmark (même ensemble de métriques)

### 3.4 Corriger la détection de fin de stream multi-runs
- [ ] Ajouter `isFinalRun: boolean` dans `BenchmarkProgress`
- [ ] Remplacer `p.p99StddevMs > 0` par `p.isFinalRun === true`

### 3.5 Corriger les `TypeError` en sweep (`.toFixed()` sur `undefined`)
- [ ] Guard `v != null ? (+v).toFixed(3) : null` avant chaque accès
- [ ] Passer `null` aux datasets Chart.js pour les points manquants

### 3.6 Corriger le tooltip Chart.js (crash sur valeur `null`)
- [ ] `label: c => c.parsed.y != null ? \`${c.dataset.label}: ${c.parsed.y.toFixed(2)} ms\` : 'N/A'`

### 3.7 Corriger le badge d'égalité (▲ attribué à Pulsar si égaux)
- [ ] `pWins = both && (m.lower ? m.kp < m.ka : m.kp > m.ka)` (false si égalité stricte)

### 3.8 Corriger le faux "Erreur SSE" à la fermeture normale
- [ ] Flag `let streamDone = false` → `true` sur dernier event ; `onerror` ne signale que si `!streamDone`

**Commits :** à venir  
**Statut : ⬜ EN ATTENTE**

---

## Sprint 4 — Qualité et enrichissement méthodologique

### 4.1 Validation des paramètres (client + serveur)
- [ ] Min/max : `messages` (1–50 000), `payloadSize` (0–65 536), `producerCount` (1–8), `runs` (1–5)
- [ ] Message d'erreur lisible dans l'UI si hors bornes

### 4.2 Barre de progression multi-run fluide
- [ ] Émettre des events `partial` pendant chaque run pour progression intra-run

### 4.3 E2E partiels Pulsar (afficher vrai 0 vs manquant)
- [ ] Passer `e2e[]` partiellement rempli dans les events `partial` (pas `new long[0]`)
- [ ] Sentinelle `-1` pour "pas encore reçu" vs `0` pour "reçu instantanément"

### 4.4 Note de durabilité dans l'UI Comparatif
- [ ] Afficher : "Artemis : journal tmpdir, fsync désactivé | Pulsar : BookKeeper standalone, fsync désactivé"

### 4.5 Option "mode séquentiel" documentée
- [ ] Si stop-and-wait conservé comme option, labelliser clairement dans l'UI

**Commits :** à venir  
**Statut : ⬜ EN ATTENTE**

---

## Suivi global

| Sprint | Thème | Statut |
|--------|-------|--------|
| Sprint 1 | Validité des mesures | 🔄 EN COURS |
| Sprint 2 | Infrastructure robuste | 🔄 EN COURS |
| Sprint 3 | IHM honnête | ⬜ EN ATTENTE |
| Sprint 4 | Qualité & enrichissement | ⬜ EN ATTENTE |

---

## Référence — Défaillances identifiées par couche

| Couche | Nb défaillances | Critiques |
|--------|----------------|-----------|
| IHM (index.html) | 21 | 4 |
| Calcul & métriques (BenchmarkService) | 13 | 2 |
| Clients broker (Artemis/Pulsar clients) | 12 | 3 |
| Infrastructure & SSE | 29 | 9 |
| **Total** | **88** | **18** |
