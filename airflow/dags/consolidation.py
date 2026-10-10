"""DAG iot_consolidation_quotidienne : extract >> quality_check >> consolidate >> export_parquet.

- Planifié chaque jour à 02:00 (UTC).
- Journée traitée = date de la *logical date* (`ds`) : la run du 02:00 du jour D traite la journée D-1
  (data interval [D-1 02:00, D 02:00), logical_date = D-1). Jamais de datetime.now() : le DAG est rejouable.
- Idempotent de bout en bout : _id déterministe + ReplaceOne (Mongo), chemin Parquet déterministe (os.replace).
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, ReplaceOne

MONGO_URL = os.getenv("MONGO_URL", "mongodb://service-db:27017/iot")
DATA_LAKE_DIR = os.getenv("DATA_LAKE_DIR", "/opt/airflow/data-lake")
ALERT_LOG_PATH = os.getenv("ALERT_LOG_PATH", "/opt/airflow/logs/alertes_airflow.log")
DAILY_MIN_MEASURES = int(os.getenv("DAILY_MIN_MEASURES", "1000"))
MAX_DLQ_RATE = 0.02
RETRY_DELAY_S = int(os.getenv("RETRY_DELAY_S", "30"))
BORNES_PHYSIQUES = {"temperature": (-40.0, 125.0), "humidite": (0.0, 100.0), "vibration": (0.0, 1000.0)}

log = logging.getLogger(__name__)


def _db():
    return MongoClient(MONGO_URL, serverSelectionTimeoutMS=10000).get_default_database()


def _bornes_jour(ds: str):
    """[début, fin[ de la journée `ds`, en datetime UTC et en ms epoch."""
    debut = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    fin = debut + timedelta(days=1)
    return debut, fin, int(debut.timestamp() * 1000), int(fin.timestamp() * 1000)


def alerte_echec(context):
    """on_failure_callback : appelé quand une tâche échoue définitivement (retries épuisés)."""
    ti = context["task_instance"]
    ligne = (f"{datetime.now(timezone.utc).isoformat()} ECHEC dag={ti.dag_id} tache={ti.task_id} "
             f"ds={context.get('ds')} tentative={ti.try_number} erreur={context.get('exception')}\n")
    os.makedirs(os.path.dirname(ALERT_LOG_PATH), exist_ok=True)
    with open(ALERT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(ligne)
    log.error(ligne.strip())


def extract(ds, **_):
    _, _, s, e = _bornes_jour(ds)
    n = _db().mesures.count_documents({"event_time": {"$gte": s, "$lt": e}})
    if n == 0:
        # exception « ordinaire » : on laisse jouer les retries (données possiblement en retard d'ingestion)
        raise ValueError(f"extract : aucune mesure brute trouvée pour la journée {ds}")
    log.info("extract %s : %s mesures", ds, n)
    return n


def quality_check(ds, **_):
    db = _db()
    debut, fin, s, e = _bornes_jour(ds)
    echecs = []

    n = db.mesures.count_documents({"event_time": {"$gte": s, "$lt": e}})
    if n < DAILY_MIN_MEASURES:
        echecs.append(f"volume insuffisant : {n} mesures < minimum {DAILY_MIN_MEASURES}")

    hors_bornes = db.mesures.count_documents({"event_time": {"$gte": s, "$lt": e}, "$or": [
        {"type": t, "$or": [{"valeur": {"$lt": lo}}, {"valeur": {"$gt": hi}}]} for t, (lo, hi) in BORNES_PHYSIQUES.items()]})
    if hors_bornes:
        echecs.append(f"{hors_bornes} valeur(s) hors bornes physiques {BORNES_PHYSIQUES}")

    n_dlq = db.dlq.count_documents({"ts": {"$gte": debut, "$lt": fin}})
    taux = n_dlq / (n + n_dlq) if (n + n_dlq) else 0.0
    if taux >= MAX_DLQ_RATE:
        echecs.append(f"taux de DLQ {taux:.2%} >= seuil {MAX_DLQ_RATE:.0%} ({n_dlq} messages rejetés)")

    if echecs:
        # échec DÉTERMINISTE des données : inutile de retenter -> AirflowFailException (pas de retry)
        raise AirflowFailException(f"quality_check {ds} BLOQUANT : " + " ; ".join(echecs))
    log.info("quality_check %s OK : %s mesures, taux DLQ %.3f%%", ds, n, taux * 100)


def consolidate(ds, **_):
    db = _db()
    _, _, s, e = _bornes_jour(ds)
    stats = db.mesures.aggregate([
        {"$match": {"event_time": {"$gte": s, "$lt": e}}},
        {"$group": {"_id": {"capteur_id": "$capteur_id", "type": "$type"}, "moyenne": {"$avg": "$valeur"},
                    "minimum": {"$min": "$valeur"}, "maximum": {"$max": "$valeur"},
                    "ecart_type": {"$stdDevSamp": "$valeur"}, "nb_mesures": {"$sum": 1}}},
        {"$sort": {"_id.capteur_id": 1, "_id.type": 1}}])
    alertes = {(d["_id"]["capteur_id"], d["_id"]["type"]): d["n"] for d in db.alertes.aggregate([
        {"$match": {"event_time": {"$gte": s, "$lt": e}}},
        {"$group": {"_id": {"capteur_id": "$capteur_id", "type": "$type"}, "n": {"$sum": 1}}}])}

    ops, ids = [], []
    for d in stats:
        cap, typ = d["_id"]["capteur_id"], d["_id"]["type"]
        _id = f"{ds}|{cap}|{typ}"
        ids.append(_id)
        doc = {"_id": _id, "date": ds, "capteur_id": cap, "type": typ, "moyenne": d["moyenne"], "minimum": d["minimum"],
               "maximum": d["maximum"], "ecart_type": d["ecart_type"], "nb_mesures": d["nb_mesures"],
               "nb_alertes": alertes.get((cap, typ), 0)}          # volontairement sans horodatage de calcul
        ops.append(ReplaceOne({"_id": _id}, doc, upsert=True))      # rejouable : même _id, même contenu
    if not ops:
        raise ValueError(f"consolidate : rien à consolider pour {ds}")
    col = db.consolidations_quotidiennes
    col.create_index([("date", 1), ("capteur_id", 1), ("type", 1)], unique=True)
    col.bulk_write(ops, ordered=False)
    col.delete_many({"date": ds, "_id": {"$nin": ids}})            # la journée == exactement le calcul courant
    log.info("consolidate %s : %s lignes", ds, len(ops))


def export_parquet(ds, **_):
    import pyarrow as pa
    import pyarrow.parquet as pq

    docs = list(_db().consolidations_quotidiennes.find({"date": ds}, {"_id": 0}).sort([("capteur_id", 1), ("type", 1)]))
    if not docs:
        raise ValueError(f"export_parquet : aucune consolidation pour {ds}")
    schema = pa.schema([("date", pa.string()), ("capteur_id", pa.string()), ("type", pa.string()),
                        ("moyenne", pa.float64()), ("minimum", pa.float64()), ("maximum", pa.float64()),
                        ("ecart_type", pa.float64()), ("nb_mesures", pa.int64()), ("nb_alertes", pa.int64())])
    table = pa.Table.from_pylist(docs, schema=schema)
    dossier = os.path.join(DATA_LAKE_DIR, f"date={ds}")            # partitionné par date
    os.makedirs(dossier, exist_ok=True)
    cible, tmp = os.path.join(dossier, "consolidation.parquet"), os.path.join(dossier, ".consolidation.parquet.tmp")
    try:
        pq.write_table(table, tmp, compression="snappy")
        os.replace(tmp, cible)                                     # atomique : un seul fichier par jour, écrasé au rejeu
    except OSError as exc:
        raise RuntimeError(f"export_parquet : écriture impossible dans {dossier} ({exc})") from exc
    with open(cible, "rb") as f:
        log.info("export_parquet %s : %s lignes -> %s (sha256=%s)", ds, table.num_rows, cible, hashlib.sha256(f.read()).hexdigest()[:16])


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=RETRY_DELAY_S),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": alerte_echec,
}

with DAG(
    dag_id="iot_consolidation_quotidienne",
    description="Consolidation quotidienne des mesures IoT (J-1), contrôle qualité, export Parquet",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,           # le rattrapage est explicite : `airflow dags backfill` (pas de rafale au démarrage)
    max_active_runs=1,
    default_args=default_args,
    tags=["iot", "batch"],
) as dag:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_quality = PythonOperator(task_id="quality_check", python_callable=quality_check)
    t_consolidate = PythonOperator(task_id="consolidate", python_callable=consolidate)
    t_export = PythonOperator(task_id="export_parquet", python_callable=export_parquet)

    t_extract >> t_quality >> t_consolidate >> t_export
