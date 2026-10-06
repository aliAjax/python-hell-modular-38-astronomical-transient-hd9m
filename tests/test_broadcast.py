import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class BroadcastTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "broadcast.db"
        self.fail_stations = set()

        def sender(delivery):
            if delivery["data"]["station_id"] in self.fail_stations:
                raise RuntimeError("station unreachable")

        self.sender = sender
        self.service = DomainService(SQLiteRepository(self.db_path), RuleEngine(), sender=sender)
        self.analyst = Actor("analyst-1", "analyst")
        self.coordinator = Actor("coordinator-1", "coordinator")
        self.source = self.service.create(
            self.analyst, "source", {"name": "Survey", "survey_name": "S"}
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _reopen(self):
        """Simulate a service restart on the same database."""
        return DomainService(SQLiteRepository(self.db_path), RuleEngine(), sender=self.sender)

    def _subscription(self, station_id, max_magnitude, source_id=None):
        data = {"station_id": station_id, "max_magnitude": max_magnitude}
        if source_id:
            data["source_id"] = source_id
        return self.service.create(self.coordinator, "subscription", data)

    def _candidate(self, magnitude=17.0, event_id="AT-1", transient_type="supernova"):
        return self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": self.source["id"],
                "event_id": event_id,
                "ra": 10,
                "dec": 20,
                "magnitude": magnitude,
                "transient_type": transient_type,
                "observed_at": "2026-09-27T00:00:00Z",
            },
        )

    def _classify(self, candidate):
        self.service.transition(
            self.analyst, candidate["id"], "triage", {"reason": "follow-up"}
        )
        return self.service.transition(
            self.analyst, candidate["id"], "classify", {"classification": "confirmed"}
        )

    def _deliveries(self, status=None):
        items = self.service.list("deliveries")
        if status:
            items = [item for item in items if item["status"] == status]
        return items

    def test_classify_fans_out_to_matching_subscriptions(self):
        self._subscription("station-a", 18.0)
        self._subscription("station-b", 15.0)  # threshold too bright for mag 17
        other_source = self.service.create(
            self.analyst, "source", {"name": "Other", "survey_name": "S2"}
        )
        self._subscription("station-c", 20.0, source_id=other_source["id"])
        paused = self._subscription("station-d", 20.0)
        self.service.transition(self.coordinator, paused["id"], "pause", {})

        candidate = self._classify(self._candidate())

        deliveries = self._deliveries("pending")
        self.assertEqual(len(deliveries), 1)
        delivery = deliveries[0]
        self.assertEqual(delivery["data"]["station_id"], "station-a")
        self.assertEqual(delivery["data"]["candidate_id"], candidate["id"])
        self.assertEqual(delivery["data"]["generated_reason"], "candidate_classified")
        self.assertTrue(delivery["data"]["batch_id"])
        self.assertEqual(delivery["data"]["attempts"], 0)
        basis = delivery["data"]["basis"]
        self.assertEqual(basis["magnitude"], 17.0)
        self.assertEqual(basis["max_magnitude"], 18.0)
        self.assertEqual(basis["source_id"], self.source["id"])
        fanout_audit = [
            entry
            for entry in self.service.audit_log(candidate["id"])
            if entry["action"] == "fanout"
        ]
        self.assertEqual(len(fanout_audit), 1)
        self.assertEqual(fanout_audit[0]["detail"]["deliveries"], 1)

    def test_same_candidate_same_station_only_once(self):
        self._subscription("station-a", 18.0)
        self._subscription("station-a", 20.0, source_id=self.source["id"])
        self._classify(self._candidate())
        deliveries = self._deliveries()
        self.assertEqual(len(deliveries), 1)

    def test_duplicate_subscription_conflicts(self):
        self._subscription("station-a", 18.0)
        with self.assertRaises(ConflictError):
            self._subscription("station-a", 19.0)

    def test_dispatch_and_station_receipts(self):
        self._subscription("station-a", 18.0)
        self._subscription("station-b", 18.0)
        self._classify(self._candidate())

        summary = self.service.dispatch_pending()
        self.assertEqual(summary, {"sent": 2, "failed": 0, "skipped": 0})

        deliveries = {d["data"]["station_id"]: d for d in self._deliveries("delivered")}
        acked = self.service.transition(
            Actor("station-a", "operator"),
            deliveries["station-a"]["id"],
            "acknowledge",
            {"note": "received"},
        )
        self.assertEqual(acked["status"], "acknowledged")
        self.assertEqual(acked["data"]["receipt"]["by"], "station-a")
        self.assertEqual(acked["data"]["receipt"]["result"], "acknowledged")
        rejected = self.service.transition(
            Actor("station-b", "operator"),
            deliveries["station-b"]["id"],
            "reject",
            {"reason": "dome closed"},
        )
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["data"]["receipt"]["reason"], "dome closed")
        with self.assertRaises(InvalidTransition):
            self.service.transition(
                Actor("station-a", "operator"),
                deliveries["station-a"]["id"],
                "acknowledge",
                {},
            )

    def test_other_station_cannot_receipt(self):
        self._subscription("station-a", 18.0)
        self._classify(self._candidate())
        self.service.dispatch_pending()
        delivery = self._deliveries("delivered")[0]
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                Actor("station-b", "operator"), delivery["id"], "acknowledge", {}
            )
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                Actor("station-b", "operator"),
                delivery["id"],
                "reject",
                {"reason": "not mine"},
            )
        # admin is not a station and may receipt on behalf of operations
        acked = self.service.transition(Actor("root", "admin"), delivery["id"], "acknowledge", {})
        self.assertEqual(acked["status"], "acknowledged")

    def test_concurrent_acknowledgement_only_one_wins(self):
        self._subscription("station-a", 18.0)
        self._classify(self._candidate())
        self.service.dispatch_pending()
        delivery = self._deliveries("delivered")[0]

        results = []
        barrier = threading.Barrier(2)

        def acknowledge():
            barrier.wait()
            try:
                self.service.transition(
                    Actor("station-a", "operator"), delivery["id"], "acknowledge", {}
                )
                results.append("ok")
            except (ConflictError, InvalidTransition):
                results.append("lost")

        threads = [threading.Thread(target=acknowledge) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(sorted(results), ["lost", "ok"])
        final = self.service.get(delivery["id"])
        self.assertEqual(final["status"], "acknowledged")
        self.assertEqual(final["version"], delivery["version"] + 1)

    def test_failed_dispatch_stays_pending_and_retry_reuses_batch(self):
        self._subscription("station-a", 18.0)
        self._subscription("station-b", 18.0)
        self._classify(self._candidate())
        batch_id = self._deliveries("pending")[0]["data"]["batch_id"]

        self.fail_stations.add("station-b")
        summary = self.service.dispatch_pending()
        self.assertEqual((summary["sent"], summary["failed"]), (1, 1))

        pending = self._deliveries("pending")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["data"]["station_id"], "station-b")
        self.assertEqual(pending[0]["data"]["attempts"], 1)
        self.assertEqual(pending[0]["data"]["last_error"], "station unreachable")
        self.assertEqual(len(self._deliveries()), 2)

        self.fail_stations.clear()
        summary = self.service.dispatch_pending(batch_id=batch_id)
        self.assertEqual((summary["sent"], summary["failed"]), (1, 0))
        self.assertEqual(len(self._deliveries()), 2)  # retry created no duplicates
        delivered = {d["data"]["station_id"]: d for d in self._deliveries("delivered")}
        self.assertEqual(delivered["station-b"]["data"]["attempts"], 2)
        self.assertEqual(delivered["station-b"]["data"]["batch_id"], batch_id)
        self.assertIsNone(delivered["station-b"]["data"]["last_error"])
        # the already delivered delivery was not sent again
        self.assertEqual(delivered["station-a"]["data"]["attempts"], 1)

    def test_withdraw_voids_unconfirmed_and_keeps_receipts(self):
        self._subscription("station-a", 18.0)
        self._subscription("station-b", 18.0)
        candidate = self._classify(self._candidate())
        self.service.dispatch_pending()
        deliveries = {d["data"]["station_id"]: d for d in self._deliveries("delivered")}
        self.service.transition(
            Actor("station-a", "operator"), deliveries["station-a"]["id"], "acknowledge", {}
        )

        self.service.transition(
            self.analyst, candidate["id"], "withdraw", {"reason": "false alarm"}
        )

        self.assertEqual(
            self.service.get(deliveries["station-a"]["id"])["status"], "acknowledged"
        )
        voided = self.service.get(deliveries["station-b"]["id"])
        self.assertEqual(voided["status"], "voided")
        self.assertEqual(voided["data"]["voided_reason"], "candidate_withdrawn")

    def test_reclassify_recomputes_unconfirmed_and_keeps_confirmed(self):
        self._subscription("station-a", 18.0)
        self._subscription("station-b", 18.0)
        candidate = self._classify(self._candidate())
        self.service.dispatch_pending()
        deliveries = {d["data"]["station_id"]: d for d in self._deliveries("delivered")}
        self.service.transition(
            Actor("station-a", "operator"), deliveries["station-a"]["id"], "acknowledge", {}
        )

        updated = self.service.transition(
            self.analyst, candidate["id"], "reclassify", {"new_type": "grb", "reason": "spectra"}
        )
        self.assertEqual(updated["status"], "classified")

        self.assertEqual(
            self.service.get(deliveries["station-a"]["id"])["status"], "acknowledged"
        )
        old_b = self.service.get(deliveries["station-b"]["id"])
        self.assertEqual(old_b["status"], "voided")
        self.assertEqual(old_b["data"]["voided_reason"], "candidate_reclassified")

        pending = self._deliveries("pending")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["data"]["station_id"], "station-b")
        self.assertEqual(pending[0]["data"]["basis"]["transient_type"], "grb")
        self.assertNotEqual(pending[0]["data"]["batch_id"], old_b["data"]["batch_id"])
        # the confirmed station is not sent a duplicate
        station_a = [d for d in self._deliveries() if d["data"]["station_id"] == "station-a"]
        self.assertEqual(len(station_a), 1)

    def test_threshold_change_recomputes_subscription(self):
        subscription = self._subscription("station-a", 15.0)
        self._classify(self._candidate())  # mag 17 fainter than 15: no delivery
        self.assertEqual(self._deliveries(), [])

        widened = self.service.transition(
            self.coordinator, subscription["id"], "update_threshold", {"max_magnitude": 18.0}
        )
        self.assertEqual(widened["status"], "active")
        self.assertEqual(widened["data"]["previous_max_magnitude"], 15.0)
        pending = self._deliveries("pending")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["data"]["basis"]["max_magnitude"], 18.0)

        self.service.dispatch_pending()
        delivered = self._deliveries("delivered")[0]
        self.service.transition(
            Actor("station-a", "operator"), delivered["id"], "acknowledge", {}
        )

        # narrowing the threshold voids nothing confirmed and generates nothing new
        self.service.transition(
            self.coordinator, subscription["id"], "update_threshold", {"max_magnitude": 16.0}
        )
        self.assertEqual(self.service.get(delivered["id"])["status"], "acknowledged")
        self.assertEqual(self._deliveries("pending"), [])
        self.assertEqual(len(self._deliveries()), 1)

    def test_threshold_narrow_voids_pending(self):
        subscription = self._subscription("station-a", 18.0)
        self._classify(self._candidate())
        pending = self._deliveries("pending")
        self.assertEqual(len(pending), 1)

        self.service.transition(
            self.coordinator, subscription["id"], "update_threshold", {"max_magnitude": 15.0}
        )
        voided = self.service.get(pending[0]["id"])
        self.assertEqual(voided["status"], "voided")
        self.assertEqual(voided["data"]["voided_reason"], "threshold_changed")
        self.assertEqual(self._deliveries("pending"), [])

    def test_new_subscription_catches_up_classified_candidates(self):
        self._classify(self._candidate())
        self.assertEqual(self._deliveries(), [])
        self._subscription("station-a", 18.0)
        pending = self._deliveries("pending")
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["data"]["generated_reason"], "subscription_created")

    def test_paused_subscription_is_skipped_until_resumed(self):
        subscription = self._subscription("station-a", 18.0)
        self.service.transition(self.coordinator, subscription["id"], "pause", {})
        self._classify(self._candidate())
        self.assertEqual(self._deliveries(), [])
        self.service.transition(self.coordinator, subscription["id"], "resume", {})
        self.assertEqual(len(self._deliveries("pending")), 1)

    def test_upgrade_backfills_missing_broadcasts_as_pending(self):
        repo = self.service.repository
        repo.create_entity(
            "legacy-candidate",
            "candidate",
            "classified",
            {
                "source_id": self.source["id"],
                "event_id": "AT-0",
                "magnitude": 16.0,
                "transient_type": "grb",
                "observed_at": "2026-09-01T00:00:00Z",
            },
            "legacy",
        )
        repo.create_entity(
            "legacy-subscription",
            "subscription",
            "active",
            {"station_id": "station-x", "max_magnitude": 18.0, "source_id": None},
            "legacy",
        )

        restarted = self._reopen()
        deliveries = [
            item
            for item in restarted.list("deliveries")
            if item["data"]["candidate_id"] == "legacy-candidate"
        ]
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "pending")
        self.assertEqual(deliveries[0]["data"]["station_id"], "station-x")
        self.assertEqual(deliveries[0]["data"]["generated_reason"], "upgrade_backfill")

        # a second restart does not backfill again
        self._reopen()
        deliveries = [
            item
            for item in restarted.list("deliveries")
            if item["data"]["candidate_id"] == "legacy-candidate"
        ]
        self.assertEqual(len(deliveries), 1)

    def test_restart_recovers_interrupted_and_pending_continue(self):
        self._subscription("station-a", 18.0)
        self._classify(self._candidate())
        pending = self._deliveries("pending")
        self.assertEqual(len(pending), 1)
        # simulate a crash between claim and send
        self.service.repository.update_entity(
            pending[0]["id"], pending[0]["version"], "sending", pending[0]["data"]
        )

        restarted = self._reopen()
        recovered = restarted.list("deliveries", status="pending")
        self.assertEqual(len(recovered), 1)
        summary = restarted.dispatch_pending()
        self.assertEqual(summary["sent"], 1)
        self.assertEqual(len(restarted.list("deliveries", status="delivered")), 1)

    def test_dispatch_requires_operator_role(self):
        self._subscription("station-a", 18.0)
        self._classify(self._candidate())
        with self.assertRaises(PermissionDenied):
            self.service.dispatch_pending(Actor("watcher", "viewer"))


if __name__ == "__main__":
    unittest.main()
