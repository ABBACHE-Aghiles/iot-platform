"""Fonctions pures (sans Spark ni Mongo) : testables unitairement (tests/test_logic.py)."""
import hashlib
import re
from datetime import datetime, timezone
from typing import List, Optional

WINDOW_MS = 30_000     # fenêtre glissante de 30 s
STEP_MS = 10_000       # pas de 10 s
MIN_OBS = 5            # observations minimales dans la fenêtre de référence
K_SIGMA = 2.0          # seuil : |v - moyenne| > 2 x écart-type
STD_EPS = 1e-9         # écart-type « nul » (bruit d'arrondi flottant)


def reference_window(event_ms: int):
    """Dernière fenêtre de 30 s COMPLÈTE se terminant au plus tard à l'instant de la mesure.

    Bornes [debut, fin[ : la mesure (event_ms >= fin) n'appartient jamais à sa propre référence,
    donc elle n'influence pas son seuil.
    """
    fin = (event_ms // STEP_MS) * STEP_MS
    return fin - WINDOW_MS, fin


def is_anomaly(valeur: float, moyenne: Optional[float], ecart_type: Optional[float], n: Optional[int]) -> bool:
    if n is None or n < MIN_OBS:
        return False                      # référence trop pauvre
    if moyenne is None or ecart_type is None or ecart_type <= STD_EPS:
        return False                      # signal constant : aucune alerte
    return abs(valeur - moyenne) > K_SIGMA * ecart_type


# --- Bonus : détection par EWMA (moyenne et variance exponentielles) -----------------------------------
EWMA_ALPHA = 0.1       # poids de la dernière mesure : mémoire effective ~ 1/alpha = 10 mesures
EWMA_HISTORY = 120     # nombre de mesures passées relues (2 min à 1 mesure / s par couple)


def ewma_stats(valeurs: List[float], alpha: float = EWMA_ALPHA):
    """Moyenne et variance exponentiellement pondérées (valeurs triées par temps croissant).

    mean_t = mean_(t-1) + alpha * (x - mean_(t-1))
    var_t  = (1 - alpha) * (var_(t-1) + alpha * (x - mean_(t-1))^2)
    Retourne (moyenne, écart-type, n), ou (None, None, 0) si la liste est vide.
    """
    if not valeurs:
        return None, None, 0
    mean, var = float(valeurs[0]), 0.0
    for x in valeurs[1:]:
        diff = x - mean
        incr = alpha * diff
        mean += incr
        var = (1.0 - alpha) * (var + diff * incr)
    return mean, var ** 0.5, len(valeurs)


def measure_id(capteur_id: str, type_mesure: str, event_ms: int, valeur: float) -> str:
    """Identifiant déterministe : deux messages strictement identiques -> même _id (dédoublonnage)."""
    return hashlib.sha1(f"{capteur_id}|{type_mesure}|{event_ms}|{float(valeur)!r}".encode()).hexdigest()


def to_dt(event_ms: int) -> datetime:
    return datetime.fromtimestamp(event_ms / 1000.0, tz=timezone.utc)


def parse_instance_index(ptr_name: str) -> Optional[int]:
    """'iot-platform-service-spark-2.iot-platform_iot-net' -> 1 (index 0-based)."""
    m = re.search(r"service-spark-(\d+)", ptr_name or "")
    return int(m.group(1)) - 1 if m else None


def partitions_for(index: int, replicas: int, total_partitions: int) -> List[int]:
    """Répartition disjointe des partitions Kafka entre instances : partition p -> instance p % replicas."""
    return [p for p in range(total_partitions) if p % replicas == index]
