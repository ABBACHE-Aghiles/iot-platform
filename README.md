# Plateforme de supervision IoT en temps réel

Pipeline **Kafka → Spark Structured Streaming → MongoDB**, consolidation batch **Airflow**, restitution par API REST (FastAPI). Tout tourne avec Docker Compose.

```
service-producer ─▶ service-kafka (topic iot.mesures, 6 partitions, clé = capteur_id)
                        └▶ service-spark ─▶ service-db (MongoDB : mesures, agregats, alertes, dlq)
                                                 ├▶ service-airflow (DAG quotidien 02:00 ─▶ Parquet data-lake/)
                                                 └▶ service-api (FastAPI, port 3001)
   + kafka-init : job éphémère qui crée le topic (6 partitions) puis s'arrête
```

## 1. Lancement

Prérequis : Docker 24+, Docker Compose v2, accès Internet au premier build (jars Kafka, images).

```bash
git clone <url-du-depot> && cd iot-platform
cp .env.example .env            # optionnel : les valeurs par défaut suffisent
docker compose up --build -d
docker compose ps               # attendre que tout soit "healthy" (Airflow : ~2 min)
```

| Service | URL / accès |
|---|---|
| **Dashboard React (bonus)** | http://localhost:8081 (via Nginx) |
| API | http://localhost:3001/docs (Swagger) · `curl localhost:3001/api/alertes?statut=active` |
| Airflow | http://localhost:8080 · utilisateur `admin`, mot de passe : `docker compose exec service-airflow cat /opt/airflow/standalone_admin_password.txt` |
| MongoDB | `mongodb://localhost:27017/iot` · `docker compose exec service-db mongosh iot` |
| Kafka | interne au réseau Docker : `service-kafka:9092` |

Tests unitaires des règles métier (sans Docker) : `python3 -m unittest tests/test_logic.py -v`

> **Windows (Git Bash)** : Docker Desktop doit tourner (backend WSL 2, virtualisation activée). Git Bash réécrit les chemins qui commencent par `/` : préfixer ces commandes par `MSYS_NO_PATHCONV=1` (exemple : `MSYS_NO_PATHCONV=1 docker compose exec service-airflow find /opt/airflow/data-lake -name '*.parquet'`). Taper les commandes à la main plutôt que de les coller évite des caractères parasites.

## 2. Démonstrations exigées

**Cas limites (2.2)**
```bash
docker compose exec -T service-producer python - --anomalie < tests/inject_edge_cases.py | tee tests/trace_inject_edge_cases.log
docker compose exec service-db mongosh iot --quiet --eval 'db.dlq.find({}, {_id:0, reason:1, raw:1}).sort({ts:-1}).limit(8)'
```
Attendu : retards ≤ 45 s intégrés aux fenêtres passées, retards de 90 s / 300 s absents des agrégats (mais présents dans `mesures`), doublon compté une fois, 6 documents `dlq`, une alerte `active` puis `resolue` ~30-40 s plus tard.

**Arrêt brutal + reprise (3.3)**
```bash
M='db.agregats.countDocuments()'
docker compose exec service-db mongosh iot --quiet --eval "$M"            # (avant)
docker kill $(docker compose ps -q service-spark)                          # (1) arrêt brutal
docker compose up -d service-spark                                         # (2) reprise MANUELLE : `docker kill` compte comme un arrêt manuel, Docker ne relance pas le conteneur
docker compose logs -f service-spark                                       # reprise sur le checkpoint, pas de "earliest" rejoué
docker compose exec service-db mongosh iot --quiet --eval "$M"            # (3) le compte ne fait que croître...
docker compose exec service-db mongosh iot --quiet --eval 'db.agregats.aggregate([{$group:{_id:{c:"$capteur_id",t:"$type",f:"$fenetre_debut"},n:{$sum:1}}},{$match:{n:{$gt:1}}}]).toArray().length'   # ...et 0 doublon
docker compose exec service-db mongosh iot --quiet --eval 'db.dlq.countDocuments()'   # (4)
```

**Backfill Airflow 3 jours (4.2)**
```bash
docker compose exec -T service-airflow python - --days 5 < tests/seed_history.py       # historique de démo
docker compose exec service-airflow airflow dags backfill -s 2026-10-03 -e 2026-10-06 iot_consolidation_quotidienne
docker compose exec service-db mongosh iot --quiet --eval 'db.consolidations_quotidiennes.aggregate([{$group:{_id:"$date",n:{$sum:1},m:{$sum:"$moyenne"}}},{$sort:{_id:1}}])'
docker compose exec service-airflow find /opt/airflow/data-lake -name '*.parquet'
# 2e exécution : --reset-dagruns est indispensable, sinon Airflow ne rejoue pas les runs déjà réussis
docker compose exec service-airflow airflow dags backfill --reset-dagruns -y -s 2026-10-03 -e 2026-10-06 iot_consolidation_quotidienne
# => mêmes agrégats, toujours 1 seul fichier .parquet par jour (à recompter)
```
(Adapter les dates aux 5 derniers jours générés par `seed_history.py`. **La borne `-e` est exclusive en pratique** : les runs sont planifiés à 02:00, donc `-e 2026-10-05` ne traite pas le 5 ; pour couvrir les 3 jours du 3 au 5, utiliser `-e 2026-10-06`.) Échec démonstratif du contrôle qualité : relancer le seed avec `--out-of-bounds <date>` puis le backfill de cette date ; le message d'échec est dans `alertes_airflow.log` (`docker compose exec service-airflow cat /opt/airflow/logs/alertes_airflow.log`).

**Passage à l'échelle (3.4)** — voir la section « Choix de scaling » ci-dessous :
```bash
SPARK_REPLICAS=2 docker compose up -d --scale service-spark=2
docker compose logs service-spark | grep -E "instance|slot"   # "instance 1/2 -> partitions [0, 2, 4]" et "instance 2/2 -> partitions [1, 3, 5]"
```
Changer le nombre de réplicas sur un volume de checkpoints existant impose de repartir de zéro : `docker compose down -v` (les partitions d'un checkpoint ne correspondraient plus). Toute commande `docker compose up` doit rester préfixée de `SPARK_REPLICAS=2`, sinon les deux instances seraient recréées avec un seul réplica.

Panne d'une instance (test du slot) :
```bash
docker kill iot-platform-service-spark-1
docker start iot-platform-service-spark-1          # même conteneur, même identifiant, donc même slot
docker compose logs service-spark | grep -E "instance|slot"   # "slot 0 acquis par <même identifiant>" puis "partitions [0, 2, 4]"
```

### Résultats observés lors des tests
| Démonstration | Résultat |
|---|---|
| Retard de 90 s | 61 mesures brutes dans la fenêtre, **60** dans l'agrégat (écarté par le watermark) |
| Retard de 45 s | 61 mesures brutes, **61** dans l'agrégat (intégré) |
| Messages malformés | 6 documents en `dlq` (`json_invalide` ×2, `champ_manquant` ×2, `valeur_non_numerique`, `type_inconnu`) |
| Arrêt brutal de Spark | 5 940 agrégats avant, 6 660 après la reprise, **0 doublon**, numéros de batch continus (reprise sur checkpoint) |
| Backfill Airflow (3 jours, rejoué 2 fois) | 90 consolidations, 3 fichiers Parquet, statistiques identiques avant/après |
| Scaling à 2 instances | `spark-1` : partitions [0, 2, 4] ; `spark-2` : [1, 3, 5] ; 0 doublon ; une instance tuée retrouve son slot |

## 2 bis. Bonus (partie 6)

Quatre fonctionnalités avancées, activées sans modifier le comportement de base.

| Bonus | Comment le vérifier |
|---|---|
| **Dashboard React temps réel (SSE)** | Ouvrir http://localhost:8081 : voyant « Flux SSE connecté », courbes moyenne/min/max qui avancent toutes les 2 s, tableau des alertes actives, choix du capteur. Flux brut : `curl -N localhost:8081/api/stream` |
| **Reverse proxy Nginx non-root** | `docker compose ps` (service-nginx healthy) · `docker inspect --format '{{.Config.User}}' iot-platform-service-nginx-1` (doit afficher un utilisateur non-root) · `curl localhost:8081/api/alertes?statut=active` · `docker compose exec service-nginx id` |
| **Détection EWMA** | `ANOMALY_DETECTOR=ewma docker compose up -d service-spark`, injecter une anomalie, puis `docker compose exec service-db mongosh iot --quiet --eval 'db.alertes.find({methode:"ewma"},{_id:0,capteur_id:1,type:1,valeur:1,statut:1}).limit(3)'`. Test unitaire : `python3 -m unittest tests/test_logic.py -v` (classe `TestEwma`). |
| **DuckDB sur le data lake** | Après un backfill Airflow : `curl localhost:3001/api/lake/resume` (synthèse sur tout l'historique) et `curl "localhost:3001/api/lake/tendance?capteur_id=capteur-1&type=temperature"` |

Reconstruire le dashboard après modification des sources (Node requis, uniquement dans ce cas) :
`cd dashboard && npm install react react-dom esbuild && npx esbuild src/main.jsx --bundle --minify --format=iife --jsx=automatic --define:process.env.NODE_ENV='"production"' --outfile=dist/app.js`

## 3. ADR — décisions d'architecture

| # | Décision | Justification / compromis |
|---|---|---|
| 1 | **Kafka en mode KRaft** (sans ZooKeeper), un seul broker, RF=1 | Moins de conteneurs, démarrage plus simple. Pas de tolérance aux pannes du broker : acceptable en local, à changer en production (≥ 3 brokers, RF=3, `min.insync.replicas=2`). |
| 2 | **`kafka-init`** crée le topic avec 6 partitions ; `auto.create.topics` désactivé | Sinon le topic serait créé à la volée avec 1 partition par défaut, et Spark pourrait démarrer avant lui. Service éphémère en plus des six services de référence. |
| 3 | **`foreachBatch` + pymongo** plutôt que le connecteur mongo-spark | Contrôle total de l'upsert sur la clé métier et de `$setOnInsert`, une dépendance de moins. Coût : `collect()` d'un micro-batch en mémoire du driver (OK à ~60 msg/s ; en production, `foreachPartition`). |
| 4 | **Image `apache/spark:3.5.1-python3`** au lieu de `apache/spark-py` | Le sujet cite `apache/spark-py` ; j'ai retenu l'image officielle `apache/spark:3.5.1-python3` (Spark 3.5.1 + Python 3, tag publié par le projet Apache Spark). Jars Kafka embarqués au build : pas d'Ivy au runtime. Le build passe brièvement root (droits des jars, `pip`) puis repasse `USER spark`. |
| 5 | **Deux requêtes streaming** (agrégats / brut+DLQ+alertes) | Les agrégats exigent un état Spark avec watermark ; la DLQ, la conservation du brut et les alertes sont plus simples et plus sûres en logique Python idempotente. Chaque requête a son checkpoint. |
| 6 | **Alertes calculées depuis la collection `mesures`**, pas depuis l'état Spark | La référence « fenêtre précédente complète sans la valeur courante » devient une simple requête bornée `[fin-30 s, fin[`. Le brut est aussi la source du batch Airflow. Coût : une requête Mongo par (capteur, type, fenêtre) et par micro-batch. |
| 7 | **Bruit uniforme borné** dans le simulateur | Avec un bruit gaussien, ~4,5 % des mesures dépasseraient 2σ (faux positifs en masse). Avec un bruit uniforme de demi-amplitude *a*, σ ≈ 0,58·a donc 2σ > a : les valeurs normales ne dépassent le seuil qu'exceptionnellement, aux bords de la plage et quand l'écart-type de la fenêtre de référence est sous-estimé (environ 0,4 % des mesures observées en test). Les anomalies injectées (pic de 8 amplitudes, au moins une toutes les 45 s) sont détectées sans ambiguïté. Limite de la règle ±2σ imposée ; piste : seuil à 3σ ou z-score glissant. |
| 8 | **Airflow `standalone`** (SQLite, SequentialExecutor), `catchup=False` | Suffisant pour l'épreuve ; `catchup=True` avec un `start_date` ancien lancerait des dizaines de runs au démarrage. Le rattrapage passe par `airflow dags backfill`. |
| 9 | **Parquet : chemin déterministe** `data-lake/date=YYYY-MM-DD/consolidation.parquet`, écriture atomique | « Horodaté » = partitionné par date. Un nom contenant l'heure de calcul créerait un nouveau fichier à chaque rejeu et casserait « un seul fichier par jour » ; aucun champ « date de calcul » n'est stocké non plus, pour que deux rejeux donnent un contenu identique. |
| 10 | **Journée traitée = date de la logical date** | Run du 02:00 le jour D : logical date = D-1, donc journée D-1 (`{{ ds }}`). Cohérent en run planifié et en backfill, sans `datetime.now()`. |
| 11 | **Nginx `nginx-unprivileged`** (7ᵉ service) devant l'API, port interne 8081 | Image officielle non-root (uid 101), port > 1024 : exigence « non-root » respectée. Un seul point d'entrée pour le dashboard (`/`) et l'API (`/api/`) : même origine, donc pas de CORS à configurer. `/api/stream` est proxifié sans buffering (`proxy_buffering off`, lecture longue) car un flux SSE bufferisé n'arrive jamais au navigateur. L'API reste aussi exposée sur 3001 comme l'exige le sujet. |
| 12 | **Dashboard React en SSE**, JS pré-construit et versionné (`dashboard/dist`) | SSE plutôt que WebSocket : flux unidirectionnel serveur -> navigateur, reconnexion automatique native, pas de contrôle d'origine à écrire. Le bundle est construit une fois avec esbuild et commité : le `docker compose up --build` n'a donc ni Node ni npm à exécuter (un build npm qui échoue casserait toute la stack). Les sources sont dans `dashboard/src`. Compromis : il faut reconstruire le bundle après une modification du code React. |
| 13 | **Détecteur EWMA** sélectionnable (`ANOMALY_DETECTOR=ewma`), seuil ±2σ conservé par défaut | La grille d'évaluation vérifie la règle 3.2 : elle reste le comportement par défaut. L'EWMA (α = 0,1, 120 dernières mesures strictement antérieures à la mesure testée) pondère davantage le présent : une dérive lente (le capteur se réchauffe doucement) déplace la moyenne de référence au lieu de déclencher une alerte, alors que la fenêtre fixe de 30 s se fait rattraper. Calcul relu en base à chaque micro-batch, sans état en mémoire : un redémarrage ne le fausse pas. Limite : demande plus d'historique pour s'établir (l'alerte exige ≥ 5 observations, comme la spec). |
| 14 | **DuckDB dans l'API** sur les fichiers Parquet (volume `data-lake` monté en lecture seule) | Requêtes analytiques sur tout l'historique sans recharger MongoDB, et sans copier de données. DuckDB est embarqué (une simple bibliothèque Python), les requêtes sont fixes et paramétrées (pas de SQL libre fourni par le client, donc pas d'injection). |

### Choix de scaling (important)
Un job Structured Streaming lit ses partitions Kafka par `assign`, **pas** par groupe de consommateurs : deux conteneurs lancés naïvement liraient chacun **les 6 partitions** (résultats dupliqués) et écriraient dans le **même checkpoint** (corruption). Solution retenue :
- chaque conteneur prend un **slot** libre (0 à N-1) via un verrou MongoDB atomique et lit un sous-ensemble disjoint de partitions (`p % SPARK_REPLICAS == slot`) ; après un `docker kill`, il retrouve son propre slot ;
- checkpoint distinct par instance (`/checkpoints/instance-<i>/`) ;
- le verrou (heartbeat 10 s, péremption 45 s) interdit à deux conteneurs actifs de prendre le même slot. Une première version déduisait l'index par résolution DNS inverse : elle a échoué en test (les deux conteneurs ont obtenu l'index 0), le verrou a empêché toute corruption, et l'attribution passe désormais par le verrou lui-même.

Limites assumées : `SPARK_REPLICAS` doit être aligné manuellement sur `--scale` ; changer le nombre d'instances exige de réinitialiser les checkpoints ; pas de rééquilibrage automatique en cas de panne définitive d'une instance (une alternative en production : Spark sur Kubernetes/YARN, où un seul driver distribue les partitions à plusieurs exécuteurs).

## 4. Réponses aux questions de réflexion

**Q1 — `depends_on: service_healthy` et reconnexion (Partie 1).** Un `depends_on` simple n'attend que le *démarrage du processus* : Kafka met plusieurs secondes à élire son contrôleur, MongoDB à ouvrir ses ports. Le producteur (`NoBrokersAvailable`) ou Spark (échec de la source Kafka / du premier upsert) planteraient au démarrage en boucle. `service_healthy` ordonne le démarrage sur des critères réels (`kafka-topics --list`, `ping` Mongo). Mais ce n'est qu'une protection de *démarrage* : si le réseau coupe en cours de vie, le code doit se défendre : producteur Kafka avec `acks=all`, `retries`, ré-création du client avec backoff exponentiel (1→30 s) ; Spark : `with_retry` (backoff exponentiel) sur chaque écriture Mongo ; si les tentatives sont épuisées, la requête échoue, le conteneur sort en erreur et `restart: unless-stopped` le relance (cela vaut pour un crash, pas pour un `docker kill`, traité par Docker comme un arrêt manuel) ; le checkpoint reprend au bon offset et l'upsert idempotent rend le rejeu inoffensif.

**Q2 — Pourquoi la clé `capteur_id` ? (Partie 2.1).** Sans clé, le partitionneur répartit les messages d'un même capteur sur plusieurs partitions, donc potentiellement sur plusieurs instances. Chaque instance calculerait alors des fenêtres *partielles* : moyenne d'un sous-ensemble, `nb_mesures` partiel, min/max/écart-type faux ; et, comme l'upsert se fait sur la même clé `(capteur_id, type, fenetre_debut)`, chaque instance **écraserait** le résultat de l'autre (le dernier gagne). L'ordre par capteur serait perdu, et la « fenêtre précédente » des alertes serait incomplète. Avec la clé, tous les messages d'un capteur vont dans la même partition, dans l'ordre, donc dans une seule instance : l'état de fenêtre est local et complet. (Dans une seule application Spark, un shuffle corrigerait le problème ; il réapparaît dès qu'on a des instances indépendantes, ce qui est le cas de `--scale`.)

**Q3 — Temps événementiel, d'arrivée, de traitement (Partie 2.1).** `event_time` : l'instant où le capteur a mesuré. Temps d'arrivée : l'instant où Kafka reçoit le message (timestamp du record). Temps de traitement : l'instant où Spark le lit. Ils diffèrent dès qu'il y a de la latence réseau, un capteur qui bufferise, un backlog, un redémarrage ou un rejeu. Le fenêtrage doit reposer sur le **temps événementiel** : il décrit le phénomène physique, donne le même résultat en rejeu, et place un retard ou un événement désordonné dans la bonne fenêtre. Le prix à payer : il faut un **watermark** pour borner l'état et décider quand une fenêtre est finale ; au-delà (ici 1 min), les événements sont écartés des agrégats.

**Q4 — Scaling et clé aléatoire (3.4).** Avec la clé `capteur_id`, une partition = un ensemble fixe de capteurs, et chaque partition est lue par une seule instance : ajouter une instance ne fait que se répartir les partitions, sans jamais scinder un capteur. Avec une clé aléatoire, les mesures d'un capteur se dispersent entre les instances : agrégats partiels écrasés (ou doublés) dans Mongo, ordre perdu, alertes calculées sur des références incomplètes. Il faudrait un `repartition` par capteur (shuffle coûteux) ou fusionner des états partiels (somme, somme des carrés, min, max, compte).

**Q5 — Batch contre streaming (Partie 4).** Les deux valeurs **divergent**. (a) Le batch pondère chaque *mesure* de la même façon ; la moyenne des moyennes de fenêtres pondère chaque *fenêtre* de la même façon, quel que soit son nombre de points, et les fenêtres glissantes se chevauchent (une mesure compte dans 3 fenêtres, sauf aux bords). (b) Les événements arrivés au-delà du watermark sont absents des fenêtres mais présents dans le brut. (c) Les fenêtres à cheval sur minuit appartiennent à deux journées. (d) Les fenêtres non finalisées ou tronquées aux bords de la période. Pour un **reporting réglementaire**, la source de vérité est le **batch** : il travaille sur le brut complet et immuable, inclut les données tardives, est reproductible et rejouable (idempotent) et auditable ; le streaming sert à l'exploitation temps réel, où la rapidité prime sur l'exactitude finale.

**Q6 — Index `{capteur_id: 1, type: 1, fenetre_debut: -1}` (Partie 5).** MongoDB utilise en général *un seul* index par requête (l'intersection d'index est rare et peu performante) ; trois index séparés obligeraient à filtrer/trier l'essentiel en mémoire. L'index composé suit la règle égalité → tri → plage : égalité sur `capteur_id` et `type`, puis lecture des fenêtres **déjà triées** de la plus récente à la plus ancienne (pas de tri en mémoire), avec un filtre de plage sur `fenetre_debut` et `limit`/`skip` efficaces. Il sert aussi les **préfixes** `(capteur_id)` et `(capteur_id, type)`, mais pas une requête sur `type` seul ou `fenetre_debut` seul. Quand l'API n'a pas de filtre `type`, elle trie `(type ↑, fenetre_debut ↓)`, exactement l'ordre de l'index.

**Volumes (TODO du compose).** `spark-checkpoints` : offsets Kafka et état des fenêtres, indispensables à la reprise après `docker kill` ; `mongo-data` : les données survivent à la recréation du conteneur ; `kafka-data` : les messages survivent au redémarrage du broker ; `data-lake` : les fichiers Parquet.

## 5. Limites connues et pistes d'amélioration
- Un seul broker Kafka (RF=1) et un seul nœud MongoDB : aucune haute disponibilité.
- `collect()` dans `foreachBatch` : passage à `foreachPartition` au-delà de quelques milliers de messages/s.
- Les alertes requêtent Mongo à chaque micro-batch : à remplacer par un état Spark (`flatMapGroupsWithState`) si le volume augmente.
- Les événements en retard > 1 min sont dans `mesures` mais pas dans `agregats` : c'est voulu, et c'est la raison d'être du batch quotidien.
- Règle ±2σ : quelques alertes sur du bruit normal (valeurs en bord de plage avec un écart-type de référence faible) ; piste d'amélioration : seuil à 3σ ou z-score glissant.
- Au redémarrage, les toutes premières fenêtres sont tronquées (ex. `nb_mesures` à 6, 26 ou 46 au lieu de 60) : une par couple capteur/type.
- Airflow `standalone` est un mode de démonstration ; production : CeleryExecutor/KubernetesExecutor + PostgreSQL.
- Dashboard : la courbe ne garde que les 60 derniers points (pré-remplis par l'API REST) ; pas d'authentification sur l'API ni sur le dashboard.
- EWMA : les paramètres (α, longueur d'historique) sont fixés dans `logic.py`, non calibrés sur des données réelles.

## 6. Structure du dépôt
```
iot-platform/
├── docker-compose.yml   ├── README.md   ├── .env.example   ├── .gitignore
├── producer/  (Dockerfile, requirements.txt, src/producer.py)
├── spark/     (Dockerfile, requirements.txt, jobs/streaming.py, jobs/logic.py)
├── airflow/   (Dockerfile, dags/consolidation.py)
├── api/       (Dockerfile, requirements.txt, src/main.py)   <- REST + SSE + DuckDB
├── nginx/     (Dockerfile, default.conf)                    <- bonus : reverse proxy non-root
├── dashboard/ (src/main.jsx, dist/ = bundle prêt à servir)  <- bonus : React temps réel
└── tests/     (inject_edge_cases.py, seed_history.py, test_logic.py)
```
