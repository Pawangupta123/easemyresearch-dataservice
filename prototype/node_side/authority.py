"""The reference Node side behind the writer's `OperationAuthority` contract — what Node's
internal endpoints will serve (api.md §3)."""

from collections.abc import Mapping

from dataservice.storage.fenced_commit import OpRef
from dataservice.storage.lease import Lease
from dataservice.storage.operations import Commit, CommittedOp, JobResult, Manifest, PublishRequest
from prototype.node_side.documents import int_at
from prototype.node_side.edit_store import EditRejected
from prototype.node_side.lease_store import MongoLeaseStore
from prototype.node_side.operation_store import MongoOperationStore
from prototype.node_side.publish_store import MongoPublishStore

Document = dict[str, object]


def commit_document(commit: Commit) -> Document:
    return {
        "baseRevision": commit.base_revision,
        "tableVersions": dict(commit.table_versions),
        "result": {
            "rowsAdded": commit.result.rows_added,
            "rowsChanged": commit.result.rows_changed,
            "changedFieldIds": list(commit.result.changed_field_ids),
            "touchedFieldIds": list(commit.result.touched_field_ids),
        },
    }


def _strings(value: object) -> tuple[str, ...]:
    return tuple(str(item) for item in value) if isinstance(value, list) else ()


def commit_from(document: Mapping[str, object]) -> Commit:
    versions, result = document.get("tableVersions"), document.get("result")
    if not isinstance(versions, Mapping) or not isinstance(result, Mapping):
        raise ValueError(f"malformed commit record: {document}")
    return Commit(
        base_revision=int_at(document, "baseRevision"),
        table_versions={str(name): int(str(version)) for name, version in versions.items()},
        result=JobResult(
            rows_added=int_at(result, "rowsAdded"),
            rows_changed=int_at(result, "rowsChanged"),
            changed_field_ids=_strings(result.get("changedFieldIds")),
            touched_field_ids=_strings(result.get("touchedFieldIds")),
        ),
    )


class NodeAuthority:
    """Every writer call is token-guarded (api.md §3): Node refuses a token that is not the
    dataset's current, live lease — a stale writer is stopped at the door, not only at Delta."""

    def __init__(self, operations: MongoOperationStore, publisher: MongoPublishStore, leases: MongoLeaseStore) -> None:
        self.operations, self.publisher, self.leases = operations, publisher, leases

    def _require_lease(self, op_id: str, token: int) -> None:
        dataset_id = self.operations.get(op_id).get("datasetId")
        live = self.leases.leases.count_documents(
            {"datasetId": dataset_id, "token": token, "$expr": {"$gt": ["$expiresAt", "$$NOW"]}}
        )
        if not live:
            raise EditRejected("FENCED", f"token {token} is not the current lease of {dataset_id}")

    def manifest(self, dataset_id: str) -> Manifest:
        db = self.operations.db
        dataset = db["datasets"].find_one({"_id": dataset_id})
        if dataset is None:
            raise ValueError(f"dataset {dataset_id} not found")
        revision = int_at(dataset, "currentRevision")
        manifest = db["dataset_revisions"].find_one({"datasetId": dataset_id, "revision": revision})
        tables = None if manifest is None else manifest.get("tables")
        if not isinstance(tables, Mapping):
            raise ValueError(f"revision {revision} has no tables")
        versions = {str(name): int_at(table, "version") for name, table in tables.items() if isinstance(table, Mapping)}
        return Manifest(revision=revision, table_versions=versions)

    def payload(self, op_id: str) -> Mapping[str, object]:
        payload = self.operations.get(op_id).get("payload")
        inline = payload.get("inline") if isinstance(payload, Mapping) else None
        if not isinstance(inline, Mapping):
            raise ValueError(f"operation {op_id} has no inline payload")
        return inline

    def running(self, op_id: str, token: int) -> int:
        self._require_lease(op_id, token)
        return self.operations.running(op_id, token)

    def committed(self, op_id: str, token: int, attempt: int, commit: Commit) -> None:
        self._require_lease(op_id, token)
        self.operations.committed(op_id, token, attempt, commit.table_versions, commit_document(commit))

    def failed(self, op_id: str, token: int, attempt: int, error: Mapping[str, str], rolled_back: bool) -> None:
        self._require_lease(op_id, token)
        self.operations.failed(op_id, token, attempt, error, rolled_back)

    def committed_operations(self, dataset_id: str) -> list[CommittedOp]:
        found = []
        for op in self.operations.committed_for(dataset_id):
            commit = op.get("commit")
            if not isinstance(commit, Mapping):
                raise ValueError(f"committed operation without its commit record: {op}")
            found.append(
                CommittedOp(OpRef(str(op["opId"]), int_at(op, "attempt")), int_at(op, "token"), commit_from(commit))
            )
        return found

    def rolled_back(self, op_id: str, attempt: int, token: int) -> None:
        self._require_lease(op_id, token)
        self.operations.rolled_back(op_id, attempt, token)

    def publish(self, request: PublishRequest, lease: Lease) -> int:
        return self.publisher.publish(request, lease)
