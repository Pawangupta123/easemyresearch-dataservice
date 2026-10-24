"""The reference Node side behind the writer's `OperationAuthority` contract — what Node's
internal endpoints will serve (api.md §3)."""

from collections.abc import Mapping

from dataservice.storage.lease import Lease
from dataservice.storage.operations import PublishRequest
from prototype.node_side.operation_store import MongoOperationStore
from prototype.node_side.publish_store import MongoPublishStore


class NodeAuthority:
    def __init__(self, operations: MongoOperationStore, publisher: MongoPublishStore) -> None:
        self.operations, self.publisher = operations, publisher

    def payload(self, op_id: str) -> Mapping[str, object]:
        payload = self.operations.get(op_id).get("payload")
        inline = payload.get("inline") if isinstance(payload, Mapping) else None
        if not isinstance(inline, Mapping):
            raise ValueError(f"operation {op_id} has no inline payload")
        return inline

    def running(self, op_id: str, token: int) -> int:
        return self.operations.running(op_id, token)

    def committed(self, op_id: str, token: int, attempt: int, table_versions: Mapping[str, int]) -> None:
        self.operations.committed(op_id, token, attempt, table_versions)

    def failed(self, op_id: str, token: int, attempt: int, error: Mapping[str, str], rolled_back: bool) -> None:
        self.operations.failed(op_id, token, attempt, error, rolled_back)

    def publish(self, request: PublishRequest, lease: Lease) -> int:
        return self.publisher.publish(request, lease)
