from uuid import uuid4

from .audit import AuditTrail
from .domain import Actor, ConflictError, NotFoundError, PermissionDenied
from .repository import utcnow
from .rules import RuleEngine, effective_magnitude, subscription_matches

SYSTEM_ACTOR = Actor("system", "system")
# Deliveries that have no station receipt yet and can still be voided on recompute.
UNCONFIRMED_DELIVERY_STATUSES = ("pending", "sending", "delivered")
DISPATCH_ROLES = ("operator", "coordinator", "admin")


class DomainService:
    def __init__(self, repository, rules=None, sender=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)
        # sender delivers one delivery to its station and raises on failure.
        self.sender = sender or (lambda delivery: None)
        self._recover_interrupted_deliveries()
        self._backfill_missing_broadcasts()

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        if kind == "subscription":
            self._catch_up_subscription(entity, "subscription_created")
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        self._after_transition(updated, action)
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # ---- broadcast ledger -------------------------------------------------

    def _after_transition(self, updated, action):
        kind = updated["kind"]
        if kind == "candidate":
            if action == "classify":
                self._fanout_candidate(updated, "candidate_classified")
            elif action == "reclassify":
                self._recompute_candidate(updated, "candidate_reclassified")
            elif action == "withdraw":
                self._void_candidate_deliveries(updated, "candidate_withdrawn")
        elif kind == "subscription":
            if action == "update_threshold":
                self._recompute_subscription(updated, "threshold_changed")
            elif action == "resume":
                self._catch_up_subscription(updated, "subscription_resumed")

    def _active_subscriptions(self):
        return self.repository.list_entities(kind="subscription", status="active")

    def _deliveries_for_candidate(self, candidate_id):
        return self.repository.find_entities("delivery", "candidate_id", candidate_id)

    def _has_open_delivery(self, candidate_id, station_id):
        for delivery in self._deliveries_for_candidate(candidate_id):
            if delivery["status"] != "voided" and delivery["data"].get("station_id") == station_id:
                return True
        return False

    def _create_delivery(self, candidate, subscription, batch_id, reason):
        station_id = subscription["data"].get("station_id")
        if self._has_open_delivery(candidate["id"], station_id):
            return None
        data = {
            "candidate_id": candidate["id"],
            "station_id": station_id,
            "subscription_id": subscription["id"],
            "batch_id": batch_id,
            "event_key": candidate["data"].get("event_key"),
            "basis": {
                "source_id": candidate["data"].get("source_id"),
                "magnitude": effective_magnitude(candidate["data"]),
                "max_magnitude": float(subscription["data"]["max_magnitude"]),
                "transient_type": candidate["data"].get("transient_type"),
                "priority_score": candidate["data"].get("priority_score"),
            },
            "attempts": 0,
            "last_error": None,
            "generated_reason": reason,
        }
        delivery_id = str(uuid4())
        delivery = self.repository.create_entity(
            delivery_id, "delivery", "pending", data, SYSTEM_ACTOR.user_id
        )
        self.audit.record(
            delivery_id,
            SYSTEM_ACTOR,
            "create",
            None,
            "pending",
            {
                "candidate_id": candidate["id"],
                "station_id": station_id,
                "batch_id": batch_id,
                "reason": reason,
            },
        )
        return delivery

    def _fanout_candidate(self, candidate, reason):
        subscriptions = [
            subscription
            for subscription in self._active_subscriptions()
            if subscription_matches(subscription["data"], candidate["data"])
        ]
        if not subscriptions:
            return []
        batch_id = "%s:%s:%s" % (reason, candidate["id"], uuid4().hex[:8])
        created = []
        for subscription in subscriptions:
            delivery = self._create_delivery(candidate, subscription, batch_id, reason)
            if delivery:
                created.append(delivery)
        self.audit.record(
            candidate["id"],
            SYSTEM_ACTOR,
            "fanout",
            candidate["status"],
            candidate["status"],
            {"batch_id": batch_id, "deliveries": len(created), "reason": reason},
        )
        return created

    def _void_deliveries(self, deliveries, reason):
        voided = []
        for delivery in deliveries:
            if delivery["status"] not in UNCONFIRMED_DELIVERY_STATUSES:
                continue
            data = dict(delivery["data"])
            data["voided_reason"] = reason
            data["voided_at"] = utcnow()
            updated = self.repository.update_entity(
                delivery["id"], delivery["version"], "voided", data
            )
            self.audit.record(
                delivery["id"], SYSTEM_ACTOR, "void", delivery["status"], "voided", {"reason": reason}
            )
            voided.append(updated)
        return voided

    def _void_candidate_deliveries(self, candidate, reason):
        return self._void_deliveries(self._deliveries_for_candidate(candidate["id"]), reason)

    def _recompute_candidate(self, candidate, reason):
        self._void_candidate_deliveries(candidate, reason)
        if candidate["status"] == "classified":
            self._fanout_candidate(candidate, reason)

    def _recompute_subscription(self, subscription, reason):
        deliveries = self.repository.find_entities("delivery", "subscription_id", subscription["id"])
        self._void_deliveries(deliveries, reason)
        self._catch_up_subscription(subscription, reason)

    def _catch_up_subscription(self, subscription, reason):
        if subscription["status"] != "active":
            return []
        created = []
        batch_id = None
        for candidate in self.repository.list_entities(kind="candidate", status="classified"):
            if not subscription_matches(subscription["data"], candidate["data"]):
                continue
            if batch_id is None:
                batch_id = "%s:%s:%s" % (reason, subscription["id"], uuid4().hex[:8])
            delivery = self._create_delivery(candidate, subscription, batch_id, reason)
            if delivery:
                created.append(delivery)
        return created

    def _recover_interrupted_deliveries(self):
        # A previous process may have crashed between claim and send; those
        # deliveries go back to pending so a restart picks them up again.
        for delivery in self.repository.list_entities(kind="delivery", status="sending"):
            self.repository.update_entity(
                delivery["id"], delivery["version"], "pending", delivery["data"]
            )
            self.audit.record(
                delivery["id"],
                SYSTEM_ACTOR,
                "recover",
                "sending",
                "pending",
                {"reason": "service_restart"},
            )

    def _backfill_missing_broadcasts(self):
        # Upgrade path: candidates confirmed before the broadcast ledger existed
        # have no delivery rows at all; generate them as pending.
        if not self._active_subscriptions():
            return
        for candidate in self.repository.list_entities(kind="candidate", status="classified"):
            if self._deliveries_for_candidate(candidate["id"]):
                continue
            self._fanout_candidate(candidate, "upgrade_backfill")

    def dispatch_pending(self, actor=None, batch_id=None):
        """Send pending deliveries, optionally limited to their original batch.

        Failed sends stay pending with the error recorded, so a later retry of
        the same batch reuses the same rows instead of creating duplicates.
        """
        if actor is not None and actor.role not in DISPATCH_ROLES:
            raise PermissionDenied("role %s cannot dispatch deliveries" % actor.role)
        summary = {"sent": 0, "failed": 0, "skipped": 0}
        for delivery in self.repository.list_entities(kind="delivery", status="pending"):
            if batch_id is not None and delivery["data"].get("batch_id") != batch_id:
                continue
            claim = dict(delivery["data"])
            claim["attempts"] = int(claim.get("attempts") or 0) + 1
            try:
                claimed = self.repository.update_entity(
                    delivery["id"], delivery["version"], "sending", claim
                )
            except (ConflictError, NotFoundError):
                summary["skipped"] += 1
                continue
            try:
                self.sender(claimed)
            except Exception as exc:
                failed = dict(claimed["data"])
                failed["last_error"] = str(exc)
                self.repository.update_entity(claimed["id"], claimed["version"], "pending", failed)
                self.audit.record(
                    claimed["id"],
                    SYSTEM_ACTOR,
                    "dispatch_failed",
                    "sending",
                    "pending",
                    {
                        "batch_id": failed.get("batch_id"),
                        "attempts": failed.get("attempts"),
                        "error": str(exc),
                    },
                )
                summary["failed"] += 1
                continue
            sent = dict(claimed["data"])
            sent["last_error"] = None
            sent["sent_at"] = utcnow()
            self.repository.update_entity(claimed["id"], claimed["version"], "delivered", sent)
            self.audit.record(
                claimed["id"],
                SYSTEM_ACTOR,
                "dispatch_sent",
                "sending",
                "delivered",
                {"batch_id": sent.get("batch_id"), "attempts": sent.get("attempts")},
            )
            summary["sent"] += 1
        return summary
