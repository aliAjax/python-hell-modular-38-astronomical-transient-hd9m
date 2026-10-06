from uuid import uuid4

from .audit import AuditTrail
from .broadcast import BroadcastLedger, SEND_ROLES
from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)
        self.broadcast = BroadcastLedger(repository, self.audit)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    @staticmethod
    def _require_role(actor, allowed):
        if actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

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
        if self.rules.normalize_kind(entity["kind"]) == "candidate":
            if action == "triage":
                self.broadcast.generate(entity_id, "confirmed", actor)
            elif action == "withdraw":
                self.broadcast.void_unconfirmed(entity_id, actor=actor)
            elif action == "reclassify":
                self.broadcast.recompute(entity_id, "reclassified", actor)
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

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------
    def create_subscription(self, actor, data):
        self._require_role(actor, ("coordinator", "admin"))
        payload = dict(data or {})
        station_id = str(payload.get("station_id", "")).strip()
        source_id = str(payload.get("source_id", "")).strip()
        if not station_id:
            raise ValidationError("station_id is required")
        if not source_id:
            raise ValidationError("source_id is required")
        if not self.repository.get_entity(source_id):
            raise ValidationError("source does not exist")
        try:
            max_magnitude = float(payload.get("max_magnitude"))
        except (TypeError, ValueError):
            raise ValidationError("max_magnitude must be numeric")
        if self.repository.find_subscription(station_id, source_id):
            raise ConflictError(
                "subscription already exists for station %s source %s" % (station_id, source_id)
            )
        subscription = self.repository.create_subscription(
            str(uuid4()), station_id, source_id, max_magnitude, actor.user_id
        )
        self.audit.record(
            subscription["id"], actor, "create_subscription", None, "active",
            {"station_id": station_id, "source_id": source_id, "max_magnitude": max_magnitude},
        )
        return subscription

    def get_subscription(self, subscription_id):
        subscription = self.repository.get_subscription(subscription_id)
        if not subscription:
            raise NotFoundError("subscription not found: " + subscription_id)
        return subscription

    def list_subscriptions(self, station_id=None, source_id=None, active=None):
        return self.repository.list_subscriptions(
            active=active, source_id=source_id, station_id=station_id
        )

    def update_subscription(self, actor, subscription_id, data, expected_version=None):
        self._require_role(actor, ("coordinator", "admin"))
        subscription = self.repository.get_subscription(subscription_id)
        if not subscription:
            raise NotFoundError("subscription not found: " + subscription_id)
        expected = int(expected_version) if expected_version is not None else subscription["version"]
        fields = {}
        payload = dict(data or {})
        if "max_magnitude" in payload:
            try:
                fields["max_magnitude"] = float(payload["max_magnitude"])
            except (TypeError, ValueError):
                raise ValidationError("max_magnitude must be numeric")
        if "active" in payload:
            fields["active"] = 1 if payload["active"] else 0
        if not fields:
            raise ValidationError("nothing to update")
        updated = self.repository.update_subscription(subscription_id, expected, **fields)
        self.audit.record(
            subscription_id, actor, "update_subscription", None, "active",
            {"fields": fields},
        )
        self.broadcast.recompute_subscription(subscription_id, actor=actor)
        return updated

    # ------------------------------------------------------------------
    # Deliveries
    # ------------------------------------------------------------------
    def get_delivery(self, delivery_id):
        delivery = self.repository.get_delivery(delivery_id)
        if not delivery:
            raise NotFoundError("delivery not found: " + delivery_id)
        return delivery

    def list_deliveries(self, candidate_id=None, station_id=None, status=None, batch_id=None):
        return self.repository.list_deliveries(
            candidate_id=candidate_id,
            station_id=station_id,
            status=status,
            batch_id=batch_id,
        )

    def list_batches(self, candidate_id=None):
        return self.repository.list_batches(candidate_id=candidate_id)

    def delivery_action(self, actor, delivery_id, action, data=None):
        payload = dict(data or {})
        if action in ("confirm", "reject"):
            return self.broadcast.receipt(delivery_id, actor, action)
        if action == "mark_sent":
            self._require_role(actor, SEND_ROLES)
            return self.broadcast.mark_sent(delivery_id, actor)
        if action == "mark_failed":
            self._require_role(actor, SEND_ROLES)
            return self.broadcast.mark_failed(delivery_id, payload.get("error", "send failed"), actor)
        if action == "retry":
            self._require_role(actor, SEND_ROLES)
            return self.broadcast.retry_delivery(delivery_id, actor)
        raise ValidationError("unknown delivery action: " + action)

    def retry_batch(self, actor, batch_id):
        self._require_role(actor, SEND_ROLES)
        return self.broadcast.retry_batch(batch_id, actor)

    # ------------------------------------------------------------------
    # Backfill / recovery
    # ------------------------------------------------------------------
    def backfill(self, actor=None):
        return self.broadcast.backfill(actor)

    def pending_deliveries(self):
        return self.broadcast.pending()
