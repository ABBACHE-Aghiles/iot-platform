"""Spark Structured Streaming : Kafka (iot.mesures) -> MongoDB.

Deux requêtes streaming indépendantes (chacune avec son checkpoint) :
  1. `agregats`  : fenêtres glissantes 30 s / pas 10 s par (capteur_id, type), temps événementiel,
                   watermark 1 min, dédoublonnage, sink MongoDB idempotent (upsert + index unique).
  2. `brut`      : mesures brutes (idempotentes), dead letter queue, détection d'anomalies
                   (spécification 3.2) et cycle de vie des alertes (active -> resolue).

Garantie : at-least-once en lecture (checkpoint) + écriture idempotente = effectively-once en sortie.
"""
import json
import logging
import os
import socket
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError, PyMongoError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import logic

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "service-kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot.mesures")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://service-db:27017/iot")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "6"))
REPLICAS = int(os.getenv("SPARK_REPLICAS", "1"))
TYPES = ["temperature", "humidite", "vibration"]
HEARTBEAT = Path("/tmp/heartbeat")
# Bonus : "sigma" = seuil +/-2 sigma sur la fenêtre précédente (spécification 3.2, défaut) ; "ewma" = moyenne
# et écart-type exponentiels sur l'historique récent (s'adapte aux dérives lentes).
ANOMALY_DETECTOR = os.getenv("ANOMALY_DETECTOR", "sigma").lower()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s streaming %(message)s")
log = logging.getLogger("streaming")

# ------------------------------------------------------------------------------------------ MongoDB
_client = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=10000, appname="iot-spark")
    return _client.get_default_database()


def utcnow():
    return datetime.now(timezone.utc)


def with_retry(fn, attempts=6):
    """Retry avec backoff exponentiel sur les erreurs Mongo. Si tout échoue, l'exception remonte,
    la requête s'arrête, le conteneur sort en erreur et `restart: unless-stopped` le relance :
    le checkpoint garantit la reprise, l'upsert garantit l'absence de doublon."""
    delay = 1
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except PyMongoError as exc:
            if attempt == attempts:
                raise
            log.warning("MongoDB indisponible (%s) - tentative %s/%s, attente %ss", exc, attempt, attempts, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def ensure_indexes(db):
    db.agregats.create_index([("capteur_id", ASCENDING), ("type", ASCENDING), ("fenetre_debut", ASCENDING)],
                             unique=True, name="uniq_cle_metier")            # clé métier de l'upsert
    db.agregats.create_index([("capteur_id", ASCENDING), ("type", ASCENDING), ("fenetre_debut", DESCENDING)],
                             name="capteur_type_fenetre_desc")               # lectures API
    db.mesures.create_index([("capteur_id", ASCENDING), ("type", ASCENDING), ("event_time", ASCENDING)])
    db.mesures.create_index([("event_time", ASCENDING)])                     # scans journaliers (Airflow)
    db.alertes.create_index([("statut", ASCENDING), ("event_time", DESCENDING)])
    db.alertes.create_index([("event_time", DESCENDING)])
    db.alertes.create_index([("capteur_id", ASCENDING), ("type", ASCENDING), ("statut", ASCENDING), ("event_time", ASCENDING)])
    db.dlq.create_index([("ts", DESCENDING)])


# ------------------------------------------------------------------------------ instance / scaling
LEASE_TTL_S = 45


def acquire_slot(db, replicas: int, holder: str):
    """Attribue à ce conteneur un slot libre parmi 0..replicas-1 grâce à un verrou MongoDB atomique.

    - Aucun DNS ni numéro de conteneur : le premier conteneur qui prend le slot 0 devient l'instance 1, etc.
    - Après `docker kill` + redémarrage, le conteneur garde son hostname : il retrouve d'abord SON slot
      (donc ses partitions et son checkpoint).
    - Un slot dont le heartbeat a plus de LEASE_TTL_S secondes est considéré abandonné et peut être repris.
    - Deux conteneurs ne peuvent jamais traiter les mêmes partitions ni partager un checkpoint.
    """
    forced = os.getenv("SPARK_INSTANCE_INDEX")
    slots = [int(forced)] if forced not in (None, "") else list(range(replicas))
    ids = [f"spark-slot-{i}" for i in slots]
    deadline = time.time() + 120
    while True:
        now = utcnow()
        mine = db.locks.find_one({"holder": holder, "_id": {"$in": ids}})
        ordre = ([mine["_id"]] if mine else []) + [i for i in ids if not mine or i != mine["_id"]]
        for lease_id in ordre:
            try:
                db.locks.find_one_and_update(
                    {"_id": lease_id, "$or": [{"holder": holder}, {"heartbeat": {"$lt": now - timedelta(seconds=LEASE_TTL_S)}}]},
                    {"$set": {"holder": holder, "heartbeat": now}},
                    upsert=True,
                )
                idx = int(lease_id.rsplit("-", 1)[1])
                log.info("slot %s acquis par %s", idx, holder)
                return idx, lease_id
            except DuplicateKeyError:
                continue                                   # slot pris par un conteneur vivant : on essaie le suivant
        if time.time() > deadline:
            log.error("tous les slots sont occupés : SPARK_REPLICAS (%s) doit correspondre à --scale service-spark=N", replicas)
            sys.exit(3)
        log.warning("aucun slot libre, nouvelle tentative dans 5 s")
        time.sleep(5)


def heartbeat_loop(spark, db, lease_id, holder):
    """Signal de vie : fichier pour le HEALTHCHECK Docker + renouvellement du verrou."""
    while True:
        actives = spark.streams.active
        if actives and all(q.isActive for q in actives):
            HEARTBEAT.touch()
            try:
                db.locks.update_one({"_id": lease_id, "holder": holder}, {"$set": {"heartbeat": utcnow()}})
            except PyMongoError as exc:
                log.warning("renouvellement du verrou impossible : %s", exc)
        time.sleep(10)


# ---------------------------------------------------------------------------------- lecture / parsing
# Tous les champs sont lus en STRING puis validés/castés explicitement : un JSON dont un champ a le
# mauvais type ne fait jamais planter le job, il part en DLQ avec la raison du rejet.
RAW_SCHEMA = T.StructType(
    [T.StructField(n, T.StringType()) for n in ("capteur_id", "type", "valeur", "event_time")]
    + [T.StructField("_corrupt", T.StringType())]
)


def read_kafka(spark, partitions):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("assign", json.dumps({TOPIC: partitions}))     # partitions propres à cette instance
        .option("startingOffsets", "earliest")                 # 1er démarrage uniquement ; ensuite : checkpoint
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )


def parse(kafka_df):
    """Renvoie un DataFrame : raw, partition, offset, capteur_id, type, valeur(double), event_ms(long), reason."""
    raw = kafka_df.select(F.col("value").cast("string").alias("raw"), F.col("partition"), F.col("offset"))
    j = raw.withColumn("j", F.from_json("raw", RAW_SCHEMA, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt"}))
    flat = j.select("raw", "partition", "offset", F.col("j._corrupt").alias("_corrupt"), F.col("j").isNull().alias("_jnull"),
                    F.col("j.capteur_id").alias("capteur_id"), F.col("j.type").alias("type"),
                    F.col("j.valeur").alias("valeur_s"), F.col("j.event_time").alias("event_time_s"))
    val = F.col("valeur_s").cast("double")
    evt = F.col("event_time_s").cast("long")
    reason = (
        F.when(F.col("raw").isNull() | F.col("_jnull") | F.col("_corrupt").isNotNull(), F.lit("json_invalide"))
        .when(F.col("capteur_id").isNull() | F.col("type").isNull() | F.col("valeur_s").isNull() | F.col("event_time_s").isNull(),
              F.lit("champ_manquant"))
        .when(val.isNull() | F.isnan(val) | val.isin(float("inf"), float("-inf")), F.lit("valeur_non_numerique"))
        .when(evt.isNull() | (evt <= 0), F.lit("event_time_invalide"))
        .when(~F.col("type").isin(*TYPES), F.lit("type_inconnu"))
    )
    return flat.select("raw", "partition", "offset", "capteur_id", "type", val.alias("valeur"),
                       evt.alias("event_ms"), reason.alias("reason"))


# -------------------------------------------------------------------- requête 1 : agrégats fenêtrés
def start_aggregates(spark, partitions, ckpt):
    valid = (
        parse(read_kafka(spark, partitions))
        .filter(F.col("reason").isNull())
        .withColumn("event_time", (F.col("event_ms") / 1000).cast("timestamp"))    # TEMPS ÉVÉNEMENTIEL
    )
    agregats = (
        valid
        .withWatermark("event_time", "1 minute")
        # doublons stricts neutralisés ; event_time dans la clé => l'état est purgé par le watermark
        .dropDuplicates(["capteur_id", "type", "valeur", "event_time"])
        .groupBy(F.col("capteur_id"), F.col("type"), F.window(F.col("event_time"), "30 seconds", "10 seconds"))
        .agg(
            F.avg("valeur").alias("moyenne"),
            F.min("valeur").alias("minimum"),
            F.max("valeur").alias("maximum"),
            F.stddev("valeur").alias("ecart_type"),
            F.count("*").alias("nb_mesures"),
        )
    )

    def upsert_aggregates(df, batch_id):
        rows = df.collect()
        if not rows:
            return
        now = utcnow()
        ops = []
        for r in rows:
            cle = {"capteur_id": r["capteur_id"], "type": r["type"], "fenetre_debut": r["window"]["start"]}
            ops.append(UpdateOne(cle, {"$set": {
                "fenetre_fin": r["window"]["end"], "moyenne": r["moyenne"], "minimum": r["minimum"],
                "maximum": r["maximum"], "ecart_type": r["ecart_type"], "nb_mesures": r["nb_mesures"],
                "updated_at": now}}, upsert=True))
        with_retry(lambda: get_db().agregats.bulk_write(ops, ordered=False))
        log.info("batch %s : %s agrégats upsertés", batch_id, len(ops))

    return (
        agregats.writeStream.outputMode("update")           # chaque fenêtre modifiée est ré-upsertée (idempotent)
        .foreachBatch(upsert_aggregates)
        .option("checkpointLocation", f"{ckpt}/agregats")
        .trigger(processingTime="10 seconds")
        .queryName("agregats")
        .start()
    )


# ------------------------------------------------------------ requête 2 : brut + DLQ + alertes
def reference_stats(db, cap, typ, ref_start, ref_end):
    docs = list(with_retry(lambda: db.mesures.aggregate([
        {"$match": {"capteur_id": cap, "type": typ, "event_time": {"$gte": ref_start, "$lt": ref_end}}},
        {"$group": {"_id": None, "n": {"$sum": 1}, "mean": {"$avg": "$valeur"}, "std": {"$stdDevSamp": "$valeur"}}},
    ])))
    return docs[0] if docs else None


def reference_stats_ewma(db, cap, typ, ref_end):
    """Statistiques EWMA calculées sur les EWMA_HISTORY dernières mesures strictement antérieures à ref_end
    (la mesure testée n'y figure donc jamais). Sans état en mémoire : un redémarrage ne change rien."""
    docs = list(with_retry(lambda: db.mesures.find(
        {"capteur_id": cap, "type": typ, "event_time": {"$lt": ref_end}}, {"_id": 0, "valeur": 1})
        .sort("event_time", DESCENDING).limit(logic.EWMA_HISTORY)))
    mean, std, n = logic.ewma_stats([d["valeur"] for d in reversed(docs)])
    return None if n == 0 else {"mean": mean, "std": std, "n": n}


def detect_alerts(db, rows, now):
    groupes = {}
    for r in rows:
        ref_start, ref_end = logic.reference_window(r["event_ms"])
        groupes.setdefault((r["capteur_id"], r["type"], ref_start, ref_end), []).append(r)
    ops = []
    for (cap, typ, ref_start, ref_end), membres in groupes.items():
        if ANOMALY_DETECTOR == "ewma":
            ref = reference_stats_ewma(db, cap, typ, ref_end)
        else:
            ref = reference_stats(db, cap, typ, ref_start, ref_end)
        if ref is None:
            continue
        for r in membres:
            if logic.is_anomaly(r["valeur"], ref["mean"], ref["std"], ref["n"]):
                ops.append(UpdateOne({"_id": logic.measure_id(cap, typ, r["event_ms"], r["valeur"])}, {"$setOnInsert": {
                    "capteur_id": cap, "type": typ, "valeur": r["valeur"], "event_time": r["event_ms"],
                    "event_ts": logic.to_dt(r["event_ms"]), "statut": "active", "created_at": now, "resolved_at": None,
                    "methode": ANOMALY_DETECTOR, "reference": {"moyenne": ref["mean"], "ecart_type": ref["std"], "nb_observations": ref["n"],
                                  "fenetre_debut": logic.to_dt(ref_start), "fenetre_fin": logic.to_dt(ref_end)}}},
                    upsert=True))          # $setOnInsert : un rejeu ne ré-active jamais une alerte déjà résolue
    if ops:
        with_retry(lambda: db.alertes.bulk_write(ops, ordered=False))
        log.info("%s alerte(s) levée(s)", len(ops))


def resolve_alerts(db, rows, now):
    """Une alerte passe à `resolue` dès qu'une fenêtre complète POSTÉRIEURE à la valeur fautive
    (>= MIN_OBS mesures) ne contient plus aucune valeur hors seuil pour le même couple."""
    dernier = {}
    for r in rows:
        k = (r["capteur_id"], r["type"])
        dernier[k] = max(dernier.get(k, 0), r["event_ms"])
    for (cap, typ), lmax in dernier.items():
        w_end = (lmax // logic.STEP_MS) * logic.STEP_MS
        w_start = w_end - logic.WINDOW_MS
        base = {"capteur_id": cap, "type": typ}
        if not db.alertes.count_documents({**base, "statut": "active", "event_time": {"$lt": w_start}}, limit=1):
            continue
        if db.alertes.count_documents({**base, "event_time": {"$gte": w_start, "$lt": w_end}}, limit=1):
            continue                                            # la fenêtre contient encore une valeur hors seuil
        if db.mesures.count_documents({**base, "event_time": {"$gte": w_start, "$lt": w_end}}, limit=logic.MIN_OBS) < logic.MIN_OBS:
            continue                                            # fenêtre trop pauvre pour conclure
        res = db.alertes.update_many({**base, "statut": "active", "event_time": {"$lt": w_start}},
                                     {"$set": {"statut": "resolue", "resolved_at": now,
                                               "resolved_window": {"debut": logic.to_dt(w_start), "fin": logic.to_dt(w_end)}}})
        if res.modified_count:
            log.info("%s alerte(s) résolue(s) pour %s/%s", res.modified_count, cap, typ)


def process_batch(df, batch_id):
    rows = df.collect()
    if not rows:
        return
    db, now = get_db(), utcnow()
    dlq_ops, mes_ops, valides = [], [], {}
    for r in rows:
        kafka = {"topic": TOPIC, "partition": r["partition"], "offset": r["offset"]}
        if r["reason"]:
            dlq_ops.append(UpdateOne({"_id": f"{TOPIC}-{r['partition']}-{r['offset']}"},
                                     {"$setOnInsert": {"raw": r["raw"], "reason": r["reason"], "kafka": kafka, "ts": now}},
                                     upsert=True))
            continue
        mid = logic.measure_id(r["capteur_id"], r["type"], r["event_ms"], r["valeur"])
        if mid in valides:
            continue                                            # doublon strict dans le même micro-batch
        valides[mid] = r
        mes_ops.append(UpdateOne({"_id": mid}, {"$setOnInsert": {
            "capteur_id": r["capteur_id"], "type": r["type"], "valeur": r["valeur"], "event_time": r["event_ms"],
            "event_ts": logic.to_dt(r["event_ms"]), "kafka": kafka}}, upsert=True))   # doublon rejoué : sans effet
    if dlq_ops:
        with_retry(lambda: db.dlq.bulk_write(dlq_ops, ordered=False))
    if mes_ops:
        with_retry(lambda: db.mesures.bulk_write(mes_ops, ordered=False))
    membres = list(valides.values())
    detect_alerts(db, membres, now)
    resolve_alerts(db, membres, now)
    log.info("batch %s : %s valides, %s en DLQ", batch_id, len(membres), len(dlq_ops))


def start_raw(spark, partitions, ckpt):
    return (
        parse(read_kafka(spark, partitions)).writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{ckpt}/brut")
        .trigger(processingTime="5 seconds")
        .queryName("brut")
        .start()
    )


# ------------------------------------------------------------------------------------------- main
def main():
    db = get_db()
    with_retry(lambda: ensure_indexes(db))
    holder = socket.gethostname()
    idx, lease_id = acquire_slot(db, REPLICAS, holder)
    partitions = logic.partitions_for(idx, REPLICAS, NUM_PARTITIONS)
    log.info("instance %s/%s -> partitions %s", idx + 1, REPLICAS, partitions)

    spark = (
        SparkSession.builder.appName(f"iot-streaming-{idx}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")      # défaut 200 = absurde pour ce volume d'état
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ckpt = f"{CHECKPOINT_DIR}/instance-{idx}"            # un checkpoint PAR instance (jamais partagé)
    start_aggregates(spark, partitions, ckpt)
    start_raw(spark, partitions, ckpt)
    threading.Thread(target=heartbeat_loop, args=(spark, db, lease_id, holder), daemon=True).start()
    spark.streams.awaitAnyTermination()                  # si une requête échoue : exit != 0 -> restart Docker


if __name__ == "__main__":
    main()
