"""Alimente MongoDB avec plusieurs journées d'historique (démo du backfill Airflow) — idempotent.

Usage :
  docker compose exec -T service-airflow python - --days 5 < tests/seed_history.py
  docker compose exec -T service-airflow python - --days 5 --out-of-bounds 2026-09-27 < tests/seed_history.py   # 1 valeur hors bornes ce jour-là
Génère, pour chacune des N journées précédant aujourd'hui (UTC), 1 mesure/minute/couple (43 200 mesures/jour),
~0,3 % de messages en DLQ et quelques alertes. _id déterministes : relancer le script ne crée aucun doublon.
"""
import argparse
import os
import random
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, ReplaceOne

p = argparse.ArgumentParser()
p.add_argument("--days", type=int, default=5)
p.add_argument("--out-of-bounds", default=None, help="YYYY-MM-DD : injecte une valeur hors bornes physiques ce jour-là")
args = p.parse_args()

db = MongoClient(os.getenv("MONGO_URL", "mongodb://service-db:27017/iot")).get_default_database()
PROFILS = {"temperature": (21.0, 1.5), "humidite": (45.0, 3.0), "vibration": (50.0, 2.0)}
rnd = random.Random(42)                       # graine fixe : jeu de données reproductible
today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

for back in range(args.days, 0, -1):
    jour = today - timedelta(days=back)
    ds = jour.strftime("%Y-%m-%d")
    mesures, dlq, alertes = [], [], []
    for minute in range(1440):
        ts = jour + timedelta(minutes=minute)
        ms = int(ts.timestamp() * 1000)
        for i in range(1, 11):
            for typ, (base, amp) in PROFILS.items():
                v = round(base + (i - 5) * 0.2 * amp + rnd.uniform(-amp, amp), 3)
                _id = f"seed|{ds}|{i}|{typ}|{minute}"
                if minute % 240 == 7 and i == 3 and typ == "temperature":      # 6 anomalies/jour
                    v = round(v + 12, 3)
                    alertes.append({"_id": _id, "capteur_id": f"capteur-{i}", "type": typ, "valeur": v, "event_time": ms,
                                    "event_ts": ts, "statut": "resolue", "created_at": ts, "resolved_at": ts + timedelta(minutes=1)})
                if args.out_of_bounds == ds and minute == 100 and i == 1 and typ == "humidite":
                    v = 250.0                                                   # humidité > 100 % : hors bornes
                mesures.append(ReplaceOne({"_id": _id}, {"_id": _id, "capteur_id": f"capteur-{i}", "type": typ, "valeur": v,
                                                        "event_time": ms, "event_ts": ts}, upsert=True))
    for k in range(130):                                                        # ~0,3 % de 43 200
        ts = jour + timedelta(seconds=k * 600)
        dlq.append(ReplaceOne({"_id": f"seed|{ds}|{k}"}, {"_id": f"seed|{ds}|{k}", "raw": "{invalide", "reason": "json_invalide", "ts": ts}, upsert=True))
    for lot in range(0, len(mesures), 10000):
        db.mesures.bulk_write(mesures[lot:lot + 10000], ordered=False)
    db.dlq.bulk_write(dlq, ordered=False)
    if alertes:
        db.alertes.bulk_write([ReplaceOne({"_id": a["_id"]}, a, upsert=True) for a in alertes], ordered=False)
    print(f"{ds} : {len(mesures)} mesures, {len(dlq)} DLQ, {len(alertes)} alertes")
db.mesures.create_index([("event_time", 1)])
print("OK")
