"""API REST de consultation (lecture seule)."""
import glob
import json
import os
import time
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pymongo import ASCENDING, DESCENDING, MongoClient

TYPES = ("temperature", "humidite", "vibration")
TYPE_PATTERN = "^(temperature|humidite|vibration)$"

DATA_LAKE_DIR = os.getenv("DATA_LAKE_DIR", "/data-lake")
LAKE_GLOB = os.path.join(DATA_LAKE_DIR, "date=*", "consolidation.parquet")
SSE_INTERVAL_S = float(os.getenv("SSE_INTERVAL_S", "2"))

client = MongoClient(os.environ["MONGO_URL"], serverSelectionTimeoutMS=5000)
db = client.get_default_database()
app = FastAPI(title="IoT Supervision API", version="1.0")


@app.on_event("startup")
def ensure_indexes():
    # idempotent : identique à celui créé par le job Spark
    db.agregats.create_index([("capteur_id", ASCENDING), ("type", ASCENDING), ("fenetre_debut", DESCENDING)],
                             name="capteur_type_fenetre_desc")


@app.get("/health")
def health():
    client.admin.command("ping")
    return {"status": "ok"}


@app.get("/api/capteurs")
def capteurs():
    """Capteurs connus + dernière mesure par type (index capteur_id/type/event_time parcouru à l'envers)."""
    out = []
    for cid in sorted(db.mesures.distinct("capteur_id"), key=lambda s: (len(s), s)):
        dernieres = {}
        for t in TYPES:
            d = db.mesures.find_one({"capteur_id": cid, "type": t}, {"_id": 0, "valeur": 1, "event_time": 1, "event_ts": 1},
                                    sort=[("event_time", DESCENDING)])
            if d:
                dernieres[t] = d
        out.append({"capteur_id": cid, "dernieres_mesures": dernieres})
    return out


@app.get("/api/capteurs/{capteur_id}/agregats")
def agregats(capteur_id: str,
             type: Optional[str] = Query(None, pattern=TYPE_PATTERN),
             debut: Optional[datetime] = None,
             fin: Optional[datetime] = None,
             page: int = Query(1, ge=1),
             taille: int = Query(50, ge=1, le=500)):
    filtre = {"capteur_id": capteur_id}
    if type:
        filtre["type"] = type
    if debut or fin:
        filtre["fenetre_debut"] = {**({"$gte": debut} if debut else {}), **({"$lt": fin} if fin else {})}
    # Le tri suit l'ordre de l'index (capteur_id, type, fenetre_debut desc) : aucun tri en mémoire.
    tri = [("fenetre_debut", DESCENDING)] if type else [("type", ASCENDING), ("fenetre_debut", DESCENDING)]
    docs = list(db.agregats.find(filtre, {"_id": 0}).sort(tri).skip((page - 1) * taille).limit(taille + 1))
    return {"page": page, "taille": taille, "has_more": len(docs) > taille, "items": docs[:taille]}


@app.get("/api/alertes")
def alertes(statut: Optional[str] = Query(None, pattern="^(active|resolue)$"),
            capteur_id: Optional[str] = None,
            limite: int = Query(100, ge=1, le=1000)):
    filtre = {}
    if statut:
        filtre["statut"] = statut
    if capteur_id:
        filtre["capteur_id"] = capteur_id
    return list(db.alertes.find(filtre, {"_id": 0}).sort("event_time", DESCENDING).limit(limite))


@app.get("/api/consolidations")
def consolidations(date: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
                   capteur_id: Optional[str] = None,
                   type: Optional[str] = Query(None, pattern=TYPE_PATTERN),
                   limite: int = Query(500, ge=1, le=5000)):
    filtre = {k: v for k, v in (("date", date), ("capteur_id", capteur_id), ("type", type)) if v}
    return list(db.consolidations_quotidiennes.find(filtre, {"_id": 0}).sort([("date", DESCENDING), ("capteur_id", ASCENDING), ("type", ASCENDING)]).limit(limite))


# ----------------------------------------------------------------------------------------------- Bonus
def _json_default(o):
    # pymongo renvoie des datetimes UTC « naïfs » : on ajoute Z pour que le navigateur les lise en UTC
    return o.isoformat() + "Z" if isinstance(o, datetime) and o.tzinfo is None else str(o)


def _snapshot():
    """Dernier agrégat de chaque couple (capteur, type) + alertes actives : une requête par couple,
    servie par l'index composé (capteur_id, type, fenetre_debut desc)."""
    agregats = []
    for cid in sorted(db.agregats.distinct("capteur_id"), key=lambda c: (len(c), c)):
        for t in TYPES:
            d = db.agregats.find_one({"capteur_id": cid, "type": t}, {"_id": 0}, sort=[("fenetre_debut", DESCENDING)])
            if d:
                agregats.append(d)
    alertes = list(db.alertes.find({"statut": "active"}, {"_id": 0, "reference": 0}).sort("event_time", DESCENDING).limit(20))
    nb_actives = db.alertes.count_documents({"statut": "active"})   # vrai total (la liste est limitée à 20)
    return {"ts": datetime.utcnow().isoformat() + "Z", "agregats": agregats, "alertes": alertes, "nb_alertes_actives": nb_actives}


@app.get("/api/stream")
def stream():
    """Server-Sent Events : pousse un instantané toutes les SSE_INTERVAL_S secondes (alimente le dashboard)."""
    def flux():
        while True:
            try:
                payload = json.dumps(_snapshot(), default=_json_default)
            except Exception as exc:                       # une erreur Mongo ne coupe pas le flux
                payload = json.dumps({"erreur": str(exc)})
            yield f"data: {payload}\n\n"
            time.sleep(SSE_INTERVAL_S)
    return StreamingResponse(flux(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _lake_query(sql: str, params=None):
    """Requête analytique DuckDB directement sur les fichiers Parquet du data lake (lecture seule)."""
    import duckdb
    if not glob.glob(LAKE_GLOB):
        raise HTTPException(404, "data lake vide : lancer d'abord le DAG (backfill)")
    con = duckdb.connect(":memory:")
    try:
        cur = con.execute(sql.replace("{lake}", f"read_parquet('{LAKE_GLOB}', hive_partitioning=false)"), params or [])
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()


@app.get("/api/lake/resume")
def lake_resume():
    """Synthèse sur tout l'historique Parquet : par (capteur, type), moyenne pondérée, extrêmes, jours, alertes."""
    return _lake_query("""
        SELECT capteur_id, type,
               count(*)                                   AS nb_jours,
               sum(nb_mesures)                            AS nb_mesures,
               sum(moyenne * nb_mesures) / sum(nb_mesures) AS moyenne,
               min(minimum)                               AS minimum,
               max(maximum)                               AS maximum,
               sum(nb_alertes)                            AS nb_alertes
        FROM {lake}
        GROUP BY capteur_id, type
        ORDER BY length(capteur_id), capteur_id, type""")


@app.get("/api/lake/tendance")
def lake_tendance(capteur_id: str, type: str = Query(..., pattern=TYPE_PATTERN)):
    """Moyenne quotidienne d'un couple (capteur, type) sur tout l'historique."""
    return _lake_query("""
        SELECT "date", moyenne, minimum, maximum, nb_mesures, nb_alertes
        FROM {lake} WHERE capteur_id = ? AND type = ? ORDER BY "date"
        """, [capteur_id, type])
