import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class BroadcastTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "broadcast.db"
        self.service = DomainService(SQLiteRepository(self.db_path), RuleEngine())
        self.analyst = Actor("analyst-1", "analyst")
        self.coordinator = Actor("coord-1", "coordinator")

    def tearDown(self):
        self.tmp.cleanup()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _source(self, name="Survey Alpha"):
        return self.service.create(
            self.analyst, "source", {"name": name, "survey_name": "ZTF-like"}
        )

    def _candidate(self, source_id, event_id="AT-1", magnitude=17.0, transient_type="supernova"):
        return self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": source_id,
                "event_id": event_id,
                "ra": 120.5,
                "dec": -12.25,
                "magnitude": magnitude,
                "transient_type": transient_type,
                "observed_at": "2026-09-27T01:00:00Z",
            },
        )

    def _subscribe(self, station_id, source_id, max_magnitude=18.0):
        return self.service.create_subscription(
            self.coordinator,
            {"station_id": station_id, "source_id": source_id, "max_magnitude": max_magnitude},
        )

    def _confirm(self, candidate):
        return self.service.transition(
            self.analyst, candidate["id"], "triage", {"reason": "confirmed"}
        )

    def _deliveries(self, candidate):
        return self.service.list_deliveries(candidate_id=candidate["id"])

    # ------------------------------------------------------------------
    # generation
    # ------------------------------------------------------------------
    def test_confirm_generates_delivery_per_matching_station(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._subscribe("station-b", source["id"])

        self._confirm(candidate)

        deliveries = self._deliveries(candidate)
        self.assertEqual(len(deliveries), 2)
        self.assertEqual({d["station_id"] for d in deliveries}, {"station-a", "station-b"})
        self.assertTrue(all(d["status"] == "pending" for d in deliveries))
        batch_ids = {d["batch_id"] for d in deliveries}
        self.assertEqual(len(batch_ids), 1)

    def test_brightness_threshold_filters_subscriptions(self):
        source = self._source()
        self._subscribe("station-a", source["id"], max_magnitude=16.0)
        faint = self._candidate(source["id"], "AT-faint", magnitude=17.5)
        bright = self._candidate(source["id"], "AT-bright", magnitude=15.0)

        self._confirm(faint)
        self.assertEqual(self._deliveries(faint), [])

        self._confirm(bright)
        deliveries = self._deliveries(bright)
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["station_id"], "station-a")

    def test_same_candidate_same_station_only_once(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._confirm(candidate)

        self.assertEqual(len(self._deliveries(candidate)), 1)
        # regenerating must not create a second active delivery
        self.service.broadcast.generate(candidate["id"], "manual")
        self.assertEqual(len(self._deliveries(candidate)), 1)
        # the database unique constraint also guards the pair
        delivery = self._deliveries(candidate)[0]
        with self.assertRaises(ConflictError):
            self.service.repository.create_delivery(
                "dup-delivery", candidate["id"], "station-a",
                delivery["subscription_id"], delivery["batch_id"],
            )

    # ------------------------------------------------------------------
    # receipts
    # ------------------------------------------------------------------
    def test_station_confirm_and_reject_leave_receipts(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._subscribe("station-b", source["id"])
        self._confirm(candidate)
        deliveries = {d["station_id"]: d for d in self._deliveries(candidate)}

        confirmed = self.service.delivery_action(
            Actor("station-a", "operator"), deliveries["station-a"]["id"], "confirm"
        )
        self.assertEqual(confirmed["status"], "confirmed")
        self.assertEqual(confirmed["receipt_action"], "confirm")
        self.assertEqual(confirmed["receipt_by"], "station-a")
        self.assertIsNotNone(confirmed["receipt_at"])

        rejected = self.service.delivery_action(
            Actor("station-b", "operator"), deliveries["station-b"]["id"], "reject"
        )
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["receipt_action"], "reject")
        self.assertEqual(rejected["receipt_by"], "station-b")

    def test_station_cannot_receipt_for_another_station(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._confirm(candidate)
        delivery = self._deliveries(candidate)[0]

        with self.assertRaises(PermissionDenied):
            self.service.delivery_action(
                Actor("station-b", "operator"), delivery["id"], "confirm"
            )

    def test_admin_can_receipt_on_behalf_of_station(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._confirm(candidate)
        delivery = self._deliveries(candidate)[0]

        confirmed = self.service.delivery_action(
            Actor("admin-1", "admin"), delivery["id"], "confirm"
        )
        self.assertEqual(confirmed["status"], "confirmed")
        self.assertEqual(confirmed["receipt_by"], "admin-1")

    def test_double_confirm_only_one_succeeds(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._confirm(candidate)
        delivery = self._deliveries(candidate)[0]
        station = Actor("station-a", "operator")

        first = self.service.delivery_action(station, delivery["id"], "confirm")
        self.assertEqual(first["status"], "confirmed")

        with self.assertRaises(ConflictError):
            self.service.delivery_action(station, delivery["id"], "confirm")

    def test_optimistic_lock_rejects_stale_receipt(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._confirm(candidate)
        delivery = self._deliveries(candidate)[0]

        # two concurrent readers both hold version 1; the first commit wins
        self.service.delivery_action(
            Actor("station-a", "operator"), delivery["id"], "confirm"
        )
        # the stale writer loses the optimistic-lock race
        with self.assertRaises(ConflictError):
            self.service.repository.transition_delivery_status(
                delivery["id"], 1, "confirmed",
                receipt_action="confirm", receipt_by="station-a",
                receipt_at="2026-09-27T02:00:00Z",
            )

    # ------------------------------------------------------------------
    # retry
    # ------------------------------------------------------------------
    def test_retry_failed_keeps_batch_and_does_not_duplicate(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._confirm(candidate)
        delivery = self._deliveries(candidate)[0]
        batch_id = delivery["batch_id"]

        failed = self.service.delivery_action(
            Actor("op-1", "operator"), delivery["id"], "mark_failed",
            {"error": "smtp timeout"},
        )
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["last_error"], "smtp timeout")

        retried = self.service.delivery_action(
            Actor("op-1", "operator"), delivery["id"], "retry"
        )
        self.assertEqual(retried["status"], "pending")
        self.assertEqual(retried["attempts"], 1)
        self.assertEqual(retried["batch_id"], batch_id)
        self.assertEqual(retried["id"], delivery["id"])

        sent = self.service.delivery_action(
            Actor("op-1", "operator"), delivery["id"], "mark_sent"
        )
        self.assertEqual(sent["status"], "sent")
        self.assertEqual(sent["attempts"], 1)
        self.assertEqual(len(self._deliveries(candidate)), 1)

    def test_retry_batch_reattempts_all_failed_in_original_batch(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._subscribe("station-b", source["id"])
        self._confirm(candidate)
        deliveries = self._deliveries(candidate)
        batch_id = deliveries[0]["batch_id"]
        op = Actor("op-1", "operator")

        for delivery in deliveries:
            self.service.delivery_action(op, delivery["id"], "mark_failed", {"error": "down"})

        result = self.service.retry_batch(op, batch_id)
        self.assertEqual(len(result["retried"]), 2)
        self.assertTrue(all(d["status"] == "pending" for d in result["retried"]))
        self.assertTrue(all(d["batch_id"] == batch_id for d in result["retried"]))
        self.assertTrue(all(d["attempts"] == 1 for d in result["retried"]))

    # ------------------------------------------------------------------
    # void / recompute
    # ------------------------------------------------------------------
    def test_withdraw_voids_unconfirmed_keeps_confirmed(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._subscribe("station-b", source["id"])
        self._confirm(candidate)
        deliveries = {d["station_id"]: d for d in self._deliveries(candidate)}

        self.service.delivery_action(
            Actor("station-a", "operator"), deliveries["station-a"]["id"], "confirm"
        )
        self.service.transition(self.analyst, candidate["id"], "withdraw", {"reason": "false positive"})

        after = {d["station_id"]: d for d in self._deliveries(candidate)}
        self.assertEqual(after["station-a"]["status"], "confirmed")
        self.assertEqual(after["station-b"]["status"], "voided")

    def test_reclassify_recomputes_unconfirmed_on_new_batch(self):
        source = self._source()
        candidate = self._candidate(source["id"], transient_type="unknown")
        self._subscribe("station-a", source["id"])
        self._subscribe("station-b", source["id"])
        self._confirm(candidate)
        first = self._deliveries(candidate)
        first_batch = first[0]["batch_id"]
        deliveries = {d["station_id"]: d for d in first}

        self.service.delivery_action(
            Actor("station-a", "operator"), deliveries["station-a"]["id"], "confirm"
        )
        self.service.transition(
            self.analyst, candidate["id"], "reclassify",
            {"new_type": "supernova", "reason": "spectrum obtained"},
        )

        all_deliveries = self._deliveries(candidate)
        station_a = [d for d in all_deliveries if d["station_id"] == "station-a"]
        station_b = [d for d in all_deliveries if d["station_id"] == "station-b"]

        # confirmed receipt survives untouched
        self.assertEqual(len(station_a), 1)
        self.assertEqual(station_a[0]["status"], "confirmed")
        # the unconfirmed delivery is voided and recalculated on a new batch
        self.assertEqual(len(station_b), 2)
        voided = [d for d in station_b if d["status"] == "voided"]
        pending = [d for d in station_b if d["status"] == "pending"]
        self.assertEqual(len(voided), 1)
        self.assertEqual(len(pending), 1)
        self.assertNotEqual(pending[0]["batch_id"], first_batch)
        self.assertNotEqual(pending[0]["id"], voided[0]["id"])

    def test_subscription_threshold_change_recomputes_deliveries(self):
        source = self._source()
        candidate = self._candidate(source["id"], magnitude=17.0)
        subscription = self._subscribe("station-a", source["id"], max_magnitude=18.0)
        self._confirm(candidate)
        self.assertEqual(len(self._deliveries(candidate)), 1)

        # tighten the threshold: candidate now falls outside, delivery is voided
        self.service.update_subscription(
            self.coordinator, subscription["id"], {"max_magnitude": 16.0}
        )
        after = self._deliveries(candidate)
        self.assertEqual(len(after), 1)
        self.assertEqual(after[0]["status"], "voided")

        # relax again: recalculated on a fresh batch
        self.service.update_subscription(
            self.coordinator, subscription["id"], {"max_magnitude": 18.0}
        )
        after = self._deliveries(candidate)
        voided = [d for d in after if d["status"] == "voided"]
        active = [d for d in after if d["status"] != "voided"]
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["status"], "pending")
        self.assertEqual(len(voided), 1)
        self.assertNotEqual(active[0]["batch_id"], voided[0]["batch_id"])

    def test_confirmed_delivery_survives_subscription_recompute(self):
        source = self._source()
        candidate = self._candidate(source["id"], magnitude=17.0)
        subscription = self._subscribe("station-a", source["id"], max_magnitude=18.0)
        self._confirm(candidate)
        delivery = self._deliveries(candidate)[0]
        self.service.delivery_action(Actor("station-a", "operator"), delivery["id"], "confirm")

        self.service.update_subscription(
            self.coordinator, subscription["id"], {"max_magnitude": 16.0}
        )
        after = self._deliveries(candidate)
        self.assertEqual(len(after), 1)
        self.assertEqual(after[0]["status"], "confirmed")

    # ------------------------------------------------------------------
    # backfill / recovery
    # ------------------------------------------------------------------
    def test_backfill_creates_pending_for_confirmed_candidates_without_deliveries(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._confirm(candidate)  # no subscriptions existed yet -> no deliveries
        self.assertEqual(self._deliveries(candidate), [])

        self._subscribe("station-a", source["id"])
        result = self.service.backfill()
        self.assertEqual(len(result["backfilled"]), 1)
        self.assertEqual(len(self._deliveries(candidate)), 1)
        self.assertEqual(self._deliveries(candidate)[0]["status"], "pending")

        # backfill is idempotent
        again = self.service.backfill()
        self.assertEqual(again["backfilled"], [])
        self.assertEqual(len(self._deliveries(candidate)), 1)

    def test_backfill_skips_unconfirmed_candidates(self):
        source = self._source()
        candidate = self._candidate(source["id"])  # still detected, not confirmed
        self._subscribe("station-a", source["id"])

        result = self.service.backfill()
        self.assertEqual(result["backfilled"], [])
        self.assertEqual(self._deliveries(candidate), [])

    def test_pending_deliveries_survive_restart(self):
        source = self._source()
        candidate = self._candidate(source["id"])
        self._subscribe("station-a", source["id"])
        self._confirm(candidate)
        delivery = self._deliveries(candidate)[0]
        self.service.delivery_action(
            Actor("op-1", "operator"), delivery["id"], "mark_failed", {"error": "down"}
        )

        # simulate a restart: a brand new service on the same database file
        restarted = DomainService(SQLiteRepository(self.db_path), RuleEngine())
        pending = restarted.pending_deliveries()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["id"], delivery["id"])

        retried = restarted.delivery_action(
            Actor("op-1", "operator"), pending[0]["id"], "retry"
        )
        self.assertEqual(retried["status"], "pending")
        self.assertEqual(retried["attempts"], 1)

    # ------------------------------------------------------------------
    # subscriptions
    # ------------------------------------------------------------------
    def test_subscription_requires_existing_source(self):
        with self.assertRaises(ValidationError):
            self.service.create_subscription(
                self.coordinator,
                {"station_id": "station-a", "source_id": "missing", "max_magnitude": 18.0},
            )

    def test_duplicate_subscription_is_conflict(self):
        source = self._source()
        self._subscribe("station-a", source["id"])
        with self.assertRaises(ConflictError):
            self._subscribe("station-a", source["id"])

    def test_viewer_cannot_manage_subscriptions(self):
        source = self._source()
        with self.assertRaises(PermissionDenied):
            self.service.create_subscription(
                Actor("viewer-1", "viewer"),
                {"station_id": "station-a", "source_id": source["id"], "max_magnitude": 18.0},
            )


if __name__ == "__main__":
    unittest.main()
