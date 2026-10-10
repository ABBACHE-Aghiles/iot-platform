"""Tests unitaires des règles métier pures : python3 -m unittest tests/test_logic.py -v"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "jobs"))
import logic  # noqa: E402


class TestAnomalies(unittest.TestCase):
    def test_reference_window_excludes_current_value(self):
        for ms in (1_000_000_000_000, 1_000_000_009_999, 1_000_000_010_000):
            debut, fin = logic.reference_window(ms)
            self.assertLessEqual(fin, ms)                    # la mesure n'est jamais dans [debut, fin[
            self.assertEqual(fin - debut, 30_000)
            self.assertEqual(fin % 10_000, 0)

    def test_seuil_2_sigma(self):
        self.assertTrue(logic.is_anomaly(30.0, 20.0, 4.0, 60))     # |10| > 8
        self.assertFalse(logic.is_anomaly(27.0, 20.0, 4.0, 60))    # |7| < 8
        self.assertFalse(logic.is_anomaly(28.0, 20.0, 4.0, 60))    # |8| == 8 : strictement supérieur requis

    def test_moins_de_5_observations(self):
        self.assertFalse(logic.is_anomaly(999.0, 20.0, 1.0, 4))
        self.assertTrue(logic.is_anomaly(999.0, 20.0, 1.0, 5))

    def test_ecart_type_nul_ou_absent(self):
        self.assertFalse(logic.is_anomaly(999.0, 20.0, 0.0, 60))
        self.assertFalse(logic.is_anomaly(999.0, 20.0, 1e-12, 60))  # bruit flottant = signal constant
        self.assertFalse(logic.is_anomaly(999.0, 20.0, None, 60))
        self.assertFalse(logic.is_anomaly(999.0, None, 1.0, 60))


class TestEwma(unittest.TestCase):
    def test_vide_et_un_point(self):
        self.assertEqual(logic.ewma_stats([]), (None, None, 0))
        self.assertEqual(logic.ewma_stats([5.0]), (5.0, 0.0, 1))

    def test_signal_constant(self):
        m, s, n = logic.ewma_stats([20.0] * 50)
        self.assertAlmostEqual(m, 20.0)
        self.assertAlmostEqual(s, 0.0)
        self.assertEqual(n, 50)

    def test_suit_une_derive_lente(self):
        # une tendance +0.1 / mesure : l'EWMA suit, une moyenne fixe sur tout l'historique est en retard
        valeurs = [20.0 + 0.1 * i for i in range(100)]
        m, s, _ = logic.ewma_stats(valeurs)
        moyenne_simple = sum(valeurs) / len(valeurs)
        self.assertLess(abs(valeurs[-1] - m), abs(valeurs[-1] - moyenne_simple))

    def test_detecte_un_pic(self):
        bruit = [20.0 + (0.3 if i % 2 else -0.3) for i in range(60)]
        m, s, n = logic.ewma_stats(bruit)
        self.assertTrue(logic.is_anomaly(32.0, m, s, n))
        self.assertFalse(logic.is_anomaly(20.1, m, s, n))


class TestIdentifiants(unittest.TestCase):
    def test_doublon_strict_meme_id(self):
        a = logic.measure_id("capteur-1", "temperature", 123, 20.5)
        self.assertEqual(a, logic.measure_id("capteur-1", "temperature", 123, 20.5))
        self.assertEqual(a, logic.measure_id("capteur-1", "temperature", 123, 20.50))
        self.assertNotEqual(a, logic.measure_id("capteur-1", "humidite", 123, 20.5))    # le type fait partie de la clé
        self.assertNotEqual(a, logic.measure_id("capteur-1", "temperature", 124, 20.5))


class TestScaling(unittest.TestCase):
    def test_partitions_disjointes_et_completes(self):
        for replicas in (1, 2, 3, 6):
            vues = [p for i in range(replicas) for p in logic.partitions_for(i, replicas, 6)]
            self.assertEqual(sorted(vues), list(range(6)))          # aucune partition perdue ni partagée

    def test_index_instance(self):
        self.assertEqual(logic.parse_instance_index("iot-platform-service-spark-1.iot-platform_iot-net"), 0)
        self.assertEqual(logic.parse_instance_index("iot-platform-service-spark-2.iot-platform_iot-net"), 1)
        self.assertIsNone(logic.parse_instance_index("autre-chose"))


if __name__ == "__main__":
    unittest.main()
