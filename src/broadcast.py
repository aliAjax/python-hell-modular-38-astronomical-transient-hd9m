from uuid import uuid4

from .domain import Actor, ConflictError, NotFoundError, PermissionDenied, ValidationError
from .repository import utcnow


CONFIRMED_STATUSES = ("triaged", "classified")
RECEIPTED_STATUSES = ("confirmed", "rejected")
ACTIVE_DELIVERY_STATUSES = ("pending", "sent", "failed")
RECEIPT_ACTIONS = ("confirm", "reject")

SEND_ROLES = ("operator", "coordinator", "admin")


def candidate_magnitude(candidate):
    measurements = candidate.get("data", {}).get("measurements") or []
    if measurements:
        latest = sorted(measurements, key=lambda m: str(m.get("observed_at", "")))[-1]
        try:
            return float(latest["magnitude"])
        except (TypeError, ValueError):
            raise ValidationError("candidate magnitude is not numeric")
    magnitude = candidate.get("data", {}).get("magnitude")
    if magnitude is None:
        raise ValidationError("candidate has no magnitude")
    return float(magnitude)


class BroadcastLedger:
    """Candidate -> subscription -> delivery receipt broadcast ledger."""

    def __init__(self, repository, audit=None):
        self.repository = repository
        self.audit = audit

    def _record(self, entity_id, actor, action, from_status, to_status, detail):
        if self.audit is None:
            return
        if actor is None:
            actor = Actor("system", "system")
        self.audit.record(entity_id, actor, action, from_status, to_status, detail)

    # ------------------------------------------------------------------
    # Matching
    # ------------------------------------------------------------------
    def matching_subscriptions(self, candidate):
        source_id = candidate.get("data", {}).get("source_id")
        if not source_id:
            return []
        magnitude = candidate_magnitude(candidate)
        subscriptions = self.repository.list_subscriptions(active=True, source_id=source_id)
        return [
            subscription
            for subscription in subscriptions
            if magnitude <= float(subscription["max_magnitude"])
        ]

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------
    def generate(self, candidate_id, reason, actor=None, batch_id=None):
        candidate = self.repository.get_entity(candidate_id)
        if not candidate:
            raise NotFoundError("candidate not found: " + candidate_id)
        subscriptions = self.matching_subscriptions(candidate)
        batch_id = batch_id or str(uuid4())
        self.repository.create_batch(batch_id, candidate_id, reason,
                                     actor.user_id if actor else None)
        created = []
        skipped = []
        for subscription in subscriptions:
            existing = self.repository.find_active_delivery(candidate_id, subscription["station_id"])
            if existing:
                skipped.append({
                    "station_id": subscription["station_id"],
                    "delivery_id": existing["id"],
                })
                continue
            delivery = self.repository.create_delivery(
                str(uuid4()),
                candidate_id,
                subscription["station_id"],
                subscription["id"],
                batch_id,
            )
            created.append(delivery)
            self._record(
                delivery["id"], actor, "broadcast.generate", None, "pending",
                {
                    "candidate_id": candidate_id,
                    "station_id": subscription["station_id"],
                    "subscription_id": subscription["id"],
                    "batch_id": batch_id,
                    "reason": reason,
                },
            )
        return {
            "batch_id": batch_id,
            "reason": reason,
            "deliveries": created,
            "skipped": skipped,
        }

    # ------------------------------------------------------------------
    # Void / recompute
    # ------------------------------------------------------------------
    def void_unconfirmed(self, candidate_id, subscription_id=None, actor=None):
        count = self.repository.void_deliveries(candidate_id, subscription_id=subscription_id)
        if count:
            self._record(
                candidate_id, actor, "broadcast.void", None, "voided",
                {
                    "candidate_id": candidate_id,
                    "subscription_id": subscription_id,
                    "count": count,
                },
            )
        return count

    def recompute(self, candidate_id, reason, actor=None):
        self.void_unconfirmed(candidate_id, actor=actor)
        return self.generate(candidate_id, reason, actor=actor)

    def recompute_subscription(self, subscription_id, actor=None):
        subscription = self.repository.get_subscription(subscription_id)
        if not subscription:
            raise NotFoundError("subscription not found: " + subscription_id)
        candidates = [
            candidate
            for candidate in self.repository.list_entities(kind="candidate")
            if candidate["status"] in CONFIRMED_STATUSES
            and candidate.get("data", {}).get("source_id") == subscription["source_id"]
        ]
        batch_id = str(uuid4())
        self.repository.create_batch(batch_id, None, "subscription_changed",
                                     actor.user_id if actor else None)
        recomputed = []
        for candidate in candidates:
            self.void_unconfirmed(candidate["id"], subscription_id=subscription_id, actor=actor)
            magnitude = candidate_magnitude(candidate)
            if magnitude > float(subscription["max_magnitude"]):
                continue
            if self.repository.find_active_delivery(candidate["id"], subscription["station_id"]):
                continue
            delivery = self.repository.create_delivery(
                str(uuid4()),
                candidate["id"],
                subscription["station_id"],
                subscription["id"],
                batch_id,
            )
            recomputed.append(delivery)
            self._record(
                delivery["id"], actor, "broadcast.generate", None, "pending",
                {
                    "candidate_id": candidate["id"],
                    "station_id": subscription["station_id"],
                    "subscription_id": subscription["id"],
                    "batch_id": batch_id,
                    "reason": "subscription_changed",
                },
            )
        return {
            "batch_id": batch_id,
            "subscription_id": subscription_id,
            "deliveries": recomputed,
        }

    # ------------------------------------------------------------------
    # Receipts
    # ------------------------------------------------------------------
    def receipt(self, delivery_id, actor, action):
        if action not in RECEIPT_ACTIONS:
            raise ValidationError("unsupported receipt action: " + action)
        delivery = self.repository.get_delivery(delivery_id)
        if not delivery:
            raise NotFoundError("delivery not found: " + delivery_id)
        if actor.role != "admin" and actor.user_id != delivery["station_id"]:
            raise PermissionDenied("station cannot receipt for another station")
        if delivery["status"] in RECEIPTED_STATUSES:
            raise ConflictError("delivery already receipted as " + delivery["status"])
        if delivery["status"] == "voided":
            raise ConflictError("delivery is voided")
        if delivery["status"] not in ACTIVE_DELIVERY_STATUSES:
            raise ConflictError("delivery cannot be receipted from status " + delivery["status"])
        next_status = "confirmed" if action == "confirm" else "rejected"
        try:
            updated = self.repository.transition_delivery_status(
                delivery_id,
                delivery["version"],
                next_status,
                receipt_action=action,
                receipt_by=actor.user_id,
                receipt_at=utcnow(),
            )
        except ConflictError:
            raise ConflictError("delivery was already receipted by another station")
        self._record(
            delivery_id, actor, "broadcast." + action, delivery["status"], next_status,
            {
                "candidate_id": delivery["candidate_id"],
                "station_id": delivery["station_id"],
            },
        )
        return updated

    # ------------------------------------------------------------------
    # Send / retry
    # ------------------------------------------------------------------
    def mark_sent(self, delivery_id, actor=None):
        delivery = self.repository.get_delivery(delivery_id)
        if not delivery:
            raise NotFoundError("delivery not found: " + delivery_id)
        if delivery["status"] not in ("pending", "failed"):
            raise ConflictError("cannot mark sent from status " + delivery["status"])
        updated = self.repository.transition_delivery_status(
            delivery_id, delivery["version"], "sent", last_error=None
        )
        self._record(
            delivery_id, actor, "broadcast.mark_sent", delivery["status"], "sent",
            {"batch_id": delivery["batch_id"]},
        )
        return updated

    def mark_failed(self, delivery_id, error, actor=None):
        delivery = self.repository.get_delivery(delivery_id)
        if not delivery:
            raise NotFoundError("delivery not found: " + delivery_id)
        if delivery["status"] not in ("pending", "sent"):
            raise ConflictError("cannot mark failed from status " + delivery["status"])
        updated = self.repository.transition_delivery_status(
            delivery_id, delivery["version"], "failed", last_error=str(error)
        )
        self._record(
            delivery_id, actor, "broadcast.mark_failed", delivery["status"], "failed",
            {"batch_id": delivery["batch_id"], "error": str(error)},
        )
        return updated

    def retry_delivery(self, delivery_id, actor=None):
        delivery = self.repository.get_delivery(delivery_id)
        if not delivery:
            raise NotFoundError("delivery not found: " + delivery_id)
        if delivery["status"] not in ("pending", "failed"):
            raise ConflictError("cannot retry from status " + delivery["status"])
        updated = self.repository.retry_delivery(delivery_id, delivery["version"])
        self._record(
            delivery_id, actor, "broadcast.retry", delivery["status"], "pending",
            {"batch_id": delivery["batch_id"], "attempt": updated["attempts"]},
        )
        return updated

    def retry_batch(self, batch_id, actor=None):
        batch = self.repository.get_batch(batch_id)
        if not batch:
            raise NotFoundError("batch not found: " + batch_id)
        deliveries = self.repository.list_deliveries(batch_id=batch_id)
        retried = []
        for delivery in deliveries:
            if delivery["status"] in ("pending", "failed"):
                retried.append(self.repository.retry_delivery(delivery["id"], delivery["version"]))
        self._record(
            batch_id, actor, "broadcast.retry_batch", None, "pending",
            {"batch_id": batch_id, "retried": [item["id"] for item in retried]},
        )
        return {"batch_id": batch_id, "retried": retried}

    # ------------------------------------------------------------------
    # Backfill / recovery
    # ------------------------------------------------------------------
    def backfill(self, actor=None):
        candidates = self.repository.list_entities(kind="candidate")
        backfilled = []
        for candidate in candidates:
            if candidate["status"] not in CONFIRMED_STATUSES:
                continue
            if self.repository.list_deliveries(candidate_id=candidate["id"]):
                continue
            backfilled.append(self.generate(candidate["id"], "backfill", actor=actor))
        return {"backfilled": backfilled}

    def pending(self):
        return self.repository.list_pending_deliveries()
