"""Simulateur de capteurs : 10 capteurs x 3 grandeurs, une mesure par couple toutes les 500 ms.

- clé Kafka = capteur_id (garantit ordre + colocalisation par capteur) ;
- event_time = temps ÉVÉNEMENTIEL (ms epoch) produit à la source ;
- au moins une anomalie injectée par minute (pic franc sur un couple aléatoire) ;
- résilience : reconnexion avec backoff exponentiel si le broker disparaît.
"""
import json
import logging
import os
import random
import signal
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "service-kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot.mesures")
NB_CAPTEURS = int(os.getenv("NB_CAPTEURS", "10"))
INTERVAL_S = float(os.getenv("INTERVAL_MS", "500")) / 1000.0
ANOMALY_EVERY_S = float(os.getenv("ANOMALY_EVERY_S", "45"))
HEARTBEAT = Path("/tmp/heartbeat")

# type -> (valeur de base, demi-amplitude du bruit borné)
# Bruit UNIFORME : son écart-type vaut a/sqrt(3) ≈ 0,58·a, donc 2σ ≈ 1,15·a > a :
# le bruit normal ne dépasse (quasiment) jamais le seuil |v-moyenne| > 2σ, seules les anomalies injectées le font.
# Avec un bruit gaussien, ~4,5 % des mesures déclencheraient une alerte (faux positifs en masse).
PROFILS = {
    "temperature": (21.0, 1.5),   # °C
    "humidite": (45.0, 3.0),      # %
    "vibration": (50.0, 2.0),     # Hz
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s producer %(message)s")
log = logging.getLogger("producer")
_stop = False


def _on_signal(signum, _frame):
    global _stop
    _stop = True
    log.info("signal %s reçu, arrêt propre", signum)


def generer_valeur(type_mesure: str, capteur: int, anomalie: bool = False) -> float:
    base, amp = PROFILS[type_mesure]
    centre = base + (capteur - NB_CAPTEURS / 2) * 0.2 * amp      # chaque capteur a son niveau propre
    valeur = centre + random.uniform(-amp, amp)
    if anomalie:
        valeur += random.choice((-1, 1)) * amp * 8                # pic franc, reste dans les bornes physiques
    return round(valeur, 3)


def make_producer() -> KafkaProducer:
    delay = 1
    while not _stop:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                acks="all",                                  # attend la confirmation du broker
                retries=5,
                max_in_flight_requests_per_connection=1,     # conserve l'ordre malgré les retries
                linger_ms=20,
                request_timeout_ms=15000,
            )
        except NoBrokersAvailable:
            log.warning("broker %s indisponible, nouvelle tentative dans %ss", BOOTSTRAP, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise SystemExit(0)


def main() -> None:
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    producer = make_producer()
    prochaine_anomalie = time.monotonic() + 10
    prochain_tick = time.monotonic()
    backoff = 1
    log.info("démarrage : %s capteurs x %s types, cadence %.0f ms", NB_CAPTEURS, len(PROFILS), INTERVAL_S * 1000)

    while not _stop:
        try:
            cible = None
            if time.monotonic() >= prochaine_anomalie:
                cible = (random.randint(1, NB_CAPTEURS), random.choice(list(PROFILS)))
                prochaine_anomalie += ANOMALY_EVERY_S
                log.info("ANOMALIE injectée sur capteur-%s/%s", *cible)
            for i in range(1, NB_CAPTEURS + 1):
                for type_mesure in PROFILS:
                    mesure = {
                        "capteur_id": f"capteur-{i}",
                        "type": type_mesure,
                        "valeur": generer_valeur(type_mesure, i, cible == (i, type_mesure)),
                        "event_time": time.time_ns() // 1_000_000,   # temps ÉVÉNEMENTIEL (ms)
                    }
                    producer.send(
                        TOPIC,
                        value=json.dumps(mesure).encode(),
                        key=mesure["capteur_id"].encode(),           # <- clé de partitionnement
                    )
            producer.flush(timeout=10)
            HEARTBEAT.touch()
            backoff = 1
        except KafkaError as exc:
            log.error("erreur Kafka (%s) : reconnexion dans %ss", exc, backoff)
            try:
                producer.close(timeout=1)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            producer = make_producer()
        prochain_tick += INTERVAL_S
        time.sleep(max(0.0, prochain_tick - time.monotonic()))

    producer.flush(timeout=5)
    producer.close()
    log.info("arrêté")


if __name__ == "__main__":
    main()
