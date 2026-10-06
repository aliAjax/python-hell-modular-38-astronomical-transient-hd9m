import unittest

from src.domain import ConflictError, ValidationError
from src.rules import (
    RuleEngine,
    calculate_priority,
    effective_magnitude,
    measurements_overlap,
    subscription_matches,
)


class RulesTest(unittest.TestCase):
    def test_priority_prefers_bright_and_high_value_events(self):
        self.assertGreater(calculate_priority(16.0, "grb"), calculate_priority(19.0, "variable"))
        with self.assertRaises(ValidationError):
            calculate_priority("not-a-number", "unknown")

    def test_overlap_is_half_open(self):
        self.assertTrue(measurements_overlap("10", "12", "11", "13"))
        self.assertFalse(measurements_overlap("10", "11", "11", "12"))

    def test_effective_magnitude_prefers_latest_measurement(self):
        candidate = {
            "magnitude": 19.0,
            "measurements": [
                {"observed_at": "2026-09-27T01:00:00Z", "magnitude": 18.0},
                {"observed_at": "2026-09-27T02:00:00Z", "magnitude": 15.5},
            ],
        }
        self.assertEqual(effective_magnitude(candidate), 15.5)
        self.assertEqual(effective_magnitude({"magnitude": 19.0}), 19.0)

    def test_subscription_matches_source_and_brightness(self):
        subscription = {"station_id": "s", "max_magnitude": 17.0, "source_id": "src-1"}
        self.assertTrue(subscription_matches(subscription, {"source_id": "src-1", "magnitude": 16.5}))
        self.assertFalse(subscription_matches(subscription, {"source_id": "src-1", "magnitude": 17.5}))
        self.assertFalse(subscription_matches(subscription, {"source_id": "src-2", "magnitude": 16.0}))
        # no source filter means every source matches
        self.assertTrue(
            subscription_matches({"max_magnitude": 17.0}, {"source_id": "src-9", "magnitude": 10.0})
        )

    def test_duplicate_event_and_measurement_are_rejected(self):
        rules = RuleEngine()
        candidate = {
            "id": "candidate-1",
            "kind": "candidate",
            "status": "detected",
            "data": {"measurements": [{"observed_at": "2026-09-27T01:00:00Z"}]},
        }
        with self.assertRaises(ConflictError):
            rules.validate_transition(
                type("Actor", (), {"role": "analyst", "user_id": "a"})(),
                candidate,
                "merge_measurement",
                {
                    "measurement": {
                        "observed_at": "2026-09-27T01:00:00Z",
                        "ra": 1,
                        "dec": 2,
                        "magnitude": 18,
                    }
                },
            )


if __name__ == "__main__":
    unittest.main()
