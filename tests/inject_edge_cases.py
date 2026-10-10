"""Injecte volontairement des cas limites dans le topic iot.mesures et trace chaque envoi.

Usage (dans le conteneur producer, qui a déjà kafka-python) :
  docker compose exec -T service-producer python - [--anomalie] < tests/inject_edge_cases.py | tee tests/trace_inject_edge_cases.log

Cas injectés (capteur-1 / temperature, valeur ~20,0 = niveau normal du capteur) :
  - retards de 5, 15, 30, 45 s           -> intégrés (retard < watermark 1 min) : nb_mesures des fenêtres passées +1
  - retards de 90 s et 300 s             -> ÉCARTÉS des agrégats par le watermark (mais conservés en brut)
  - événements désordonnés               -> event_time non monotone pour un même capteur : bien fenêtrés
  - doublon strict (même message x2)     -> compté une seule fois
  - malformés (JSON invalide, champ manquant, valeur non numérique, type inconnu) -> collection dlq
  - --anomalie                           -> un pic franc (valeur 8 amplitudes au-dessus) -> alerte active
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "service-kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot.mesures")
CAP, TYPE, VALEUR = "capteur-1", "temperature", 20.0

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", retries=5, max_in_flight_requests_per_connection=1)


def now_ms():
    return int(time.time() * 1000)


def trace(label, payload):
    print(f"{datetime.now(timezone.utc).isoformat()} | {label:<34} | {payload}", flush=True)


def send_raw(label, raw: bytes, key: str = CAP):
    producer.send(TOPIC, value=raw, key=key.encode())
    trace(label, raw.decode(errors="replace"))


def send_measure(label, event_ms, valeur=VALEUR, **override):
    m = {"capteur_id": CAP, "type": TYPE, "valeur": valeur, "event_time": event_ms, **override}
    send_raw(label, json.dumps(m).encode())
    return m


def main():
    trace("DEBUT", f"topic={TOPIC} bootstrap={BOOTSTRAP}")

    for i, d in enumerate((5, 15, 30, 45)):
        send_measure(f"RETARD {d}s (attendu: intégré)", now_ms() - d * 1000, round(VALEUR + 0.01 * i, 3))
    for d in (90, 300):
        send_measure(f"RETARD {d}s (attendu: écarté)", now_ms() - d * 1000, round(VALEUR + 0.05, 3))

    base = now_ms()
    for i, dt in enumerate((1000, 6000, 3000, 8000)):     # 1 s, puis 6 s, puis 3 s, puis 8 s dans le passé
        send_measure(f"DESORDRE t-{dt // 1000}s (attendu: intégré)", base - dt, round(VALEUR + 0.02 * i, 3))

    doublon = json.dumps({"capteur_id": CAP, "type": TYPE, "valeur": 20.123, "event_time": now_ms() - 2000}).encode()
    send_raw("DOUBLON 1/2", doublon)
    send_raw("DOUBLON 2/2 (attendu: 1 seul compté)", doublon)

    t = now_ms()
    send_raw("MALFORME json invalide", b'{"capteur_id": "capteur-1", "type": "temperature", ')
    send_raw("MALFORME champ manquant (valeur)", json.dumps({"capteur_id": CAP, "type": TYPE, "event_time": t}).encode())
    send_raw("MALFORME valeur non numérique", json.dumps({"capteur_id": CAP, "type": TYPE, "valeur": "abc", "event_time": t}).encode())
    send_raw("MALFORME valeur null", json.dumps({"capteur_id": CAP, "type": TYPE, "valeur": None, "event_time": t}).encode())
    send_raw("MALFORME type inconnu", json.dumps({"capteur_id": CAP, "type": "pression", "valeur": 1.0, "event_time": t}).encode())
    send_raw("MALFORME pas un objet JSON", b"[1, 2, 3]")

    if "--anomalie" in sys.argv:
        send_measure("ANOMALIE (attendu: alerte active)", now_ms(), VALEUR + 12.0)

    producer.flush(timeout=15)
    trace("FIN", "tout est publié (kafka-python : flush OK)")


main()
