"""Node's side of the writer protocol over HTTP (api.md §3) — the production `LeaseAuthority` and
`OperationAuthority`; prototype/node_side is the same logic in-process, for the scenario tests.

Every call is signed: HMAC-SHA256 over the timestamp, the method, the path with its query and the
body's exact bytes (Node checks it before parsing anything). Node answers a refusal with a code:
FENCED stops the writer at once (`FenceError`), OPERATION_FAILED moves it to the next operation
(`OperationFailedError`); any other refusal or a network failure is an ordinary exception, so an
attempt that hits one is rolled back and retried, as with the reference Node side.
"""

import hashlib
import hmac
import json
import time
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from dataservice.common.errors import FenceError, OperationFailedError
from dataservice.storage.fenced_commit import OpRef
from dataservice.storage.lease import Lease
from dataservice.storage.operations import Commit, CommittedOp, JobResult, Manifest, PublishRequest, QueuedOp

TIMESTAMP_HEADER = "x-dataservice-timestamp"
SIGNATURE_HEADER = "x-dataservice-signature"

Json = object


class NodeRejected(Exception):
    """Node refused the call (CONFLICT, NOT_FOUND, …) — `code` says why."""

    def __init__(self, code: str, status: int, message: str) -> None:
        super().__init__(f"Node refused ({status} {code}): {message}")
        self.code, self.status = code, status


class NodeUnavailable(Exception):
    """Node could not be reached or answered something that is not its envelope."""


def sign(secret: str, timestamp: str, method: str, path: str, body: bytes) -> str:
    """The hex signature Node's dataservice-signature middleware checks."""
    message = f"{timestamp}\n{method.upper()}\n{path}\n".encode() + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def _commit_body(commit: Commit) -> dict[str, Json]:
    result = commit.result
    return {
        "baseRevision": commit.base_revision,
        "tableVersions": dict(commit.table_versions),
        "result": {
            "rowsAdded": result.rows_added,
            "rowsChanged": result.rows_changed,
            "changedFieldIds": list(result.changed_field_ids),
            "touchedFieldIds": list(result.touched_field_ids),
        },
    }


def _int(document: Mapping[str, Json], key: str) -> int:
    value = document.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise NodeUnavailable(f"Node answered a non-integer {key}: {document}")
    return value


def _mapping(value: Json, what: str) -> Mapping[str, Json]:
    if not isinstance(value, Mapping):
        raise NodeUnavailable(f"Node answered {what} that is not an object: {value!r}")
    return cast(Mapping[str, Json], value)


def _strings(document: Mapping[str, Json], key: str) -> tuple[str, ...]:
    value = document.get(key)
    return tuple(str(item) for item in value) if isinstance(value, list) else ()


def _commit_from(document: Mapping[str, Json]) -> Commit:
    versions = _mapping(document.get("tableVersions"), "table versions")
    result = _mapping(document.get("result"), "a job result")
    return Commit(
        base_revision=_int(document, "baseRevision"),
        table_versions={str(name): _int(versions, str(name)) for name in versions},
        result=JobResult(
            rows_added=_int(result, "rowsAdded"),
            rows_changed=_int(result, "rowsChanged"),
            changed_field_ids=_strings(result, "changedFieldIds"),
            touched_field_ids=_strings(result, "touchedFieldIds"),
        ),
    )


class NodeAuthority:
    def __init__(
        self,
        base_url: str,
        secret: str,
        timeout_seconds: float,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._path = urlsplit(self._base).path
        self._secret, self._timeout, self._clock = secret, timeout_seconds, clock

    # ── transport ──────────────────────────────────────────────────────────────

    def _call(self, method: str, path: str, body: Mapping[str, Json] | None = None) -> Json:
        raw = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
        timestamp = str(int(self._clock() * 1000))
        headers = {
            "content-type": "application/json",
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: sign(self._secret, timestamp, method, self._path + path, raw),
        }
        request = Request(self._base + path, data=raw if method == "POST" else None, method=method, headers=headers)
        try:
            with urlopen(request, timeout=self._timeout) as response:  # noqa: S310 — the URL is Node's, from settings
                envelope = json.loads(response.read())
        except HTTPError as error:
            raise self._rejection(error) from None
        except (URLError, TimeoutError, json.JSONDecodeError) as error:
            raise NodeUnavailable(f"{method} {path}: {error}") from error
        return _mapping(envelope, "an envelope").get("data")

    @staticmethod
    def _rejection(error: HTTPError) -> Exception:
        try:
            envelope = _mapping(json.loads(error.read()), "an error envelope")
        except (json.JSONDecodeError, NodeUnavailable):
            return NodeUnavailable(f"Node answered {error.code} without its envelope")
        data = envelope.get("data")
        code = str(data.get("code")) if isinstance(data, Mapping) else "UNKNOWN"
        message = str(envelope.get("message") or "")
        if code == "FENCED":
            return FenceError(f"Node fenced this writer: {message}", user_safe=False)
        if code == "OPERATION_FAILED":
            return OperationFailedError(f"Node failed the operation: {message}", user_safe=False)
        return NodeRejected(code, error.code, message)

    @staticmethod
    def _id(value: str) -> str:
        return quote(value, safe="")

    # ── LeaseAuthority ─────────────────────────────────────────────────────────

    def _lease(self, dataset_id: str, owner: str, body: Mapping[str, Json]) -> Lease | None:
        data = self._call("POST", f"/datasets/{self._id(dataset_id)}/lease", body)
        if data is None:
            return None
        grant = _mapping(data, "a lease")
        expires = datetime.fromisoformat(str(grant.get("expiresAt")))
        return Lease(dataset_id=dataset_id, owner=owner, token=_int(grant, "token"), expires_at=expires)

    def acquire(self, dataset_id: str, owner: str, max_token_seen: int) -> Lease | None:
        return self._lease(dataset_id, owner, {"action": "acquire", "owner": owner, "maxTokenSeen": max_token_seen})

    def renew(self, lease: Lease) -> Lease | None:
        return self._lease(
            lease.dataset_id, lease.owner, {"action": "renew", "owner": lease.owner, "token": lease.token}
        )

    def release(self, lease: Lease) -> None:
        body = {"action": "release", "owner": lease.owner, "token": lease.token}
        self._call("POST", f"/datasets/{self._id(lease.dataset_id)}/lease", body)

    # ── OperationAuthority ─────────────────────────────────────────────────────

    def manifest(self, dataset_id: str) -> Manifest:
        data = _mapping(self._call("GET", f"/datasets/{self._id(dataset_id)}/manifest"), "a manifest")
        versions = _mapping(data.get("tableVersions"), "table versions")
        return Manifest(
            revision=_int(data, "revision"), table_versions={str(k): _int(versions, str(k)) for k in versions}
        )

    def next_operation(self, dataset_id: str, token: int) -> QueuedOp | None:
        data = self._call("GET", f"/datasets/{self._id(dataset_id)}/operations/next?token={token}")
        if data is None:
            return None
        op = _mapping(data, "an operation")
        return QueuedOp(op_id=str(op.get("opId")), op_type=str(op.get("type")))

    def payload(self, op_id: str) -> Mapping[str, object]:
        return _mapping(self._call("GET", f"/operations/{self._id(op_id)}/payload"), "a payload")

    def running(self, op_id: str, token: int) -> int:
        data = _mapping(self._call("POST", f"/operations/{self._id(op_id)}/running", {"token": token}), "an attempt")
        return _int(data, "attempt")

    def committed(self, op_id: str, token: int, attempt: int, commit: Commit) -> None:
        body = {"token": token, "attempt": attempt, "commit": _commit_body(commit)}
        self._call("POST", f"/operations/{self._id(op_id)}/committed", body)

    def failed(self, op_id: str, token: int, attempt: int, error: Mapping[str, str], retryable: bool) -> bool:
        body = {
            "token": token,
            "attempt": attempt,
            "error": {"code": error.get("code", "OPERATION_FAILED"), "message": error.get("message", "")[:2000]},
            "retryable": retryable,
        }
        data = _mapping(self._call("POST", f"/operations/{self._id(op_id)}/failed", body), "a failure answer")
        return data.get("final") is True

    def committed_operations(self, dataset_id: str) -> list[CommittedOp]:
        data = self._call("GET", f"/datasets/{self._id(dataset_id)}/operations/committed")
        found = []
        for item in cast(list[Json], data or []):
            op = _mapping(item, "a committed operation")
            commit = _commit_from(_mapping(op.get("commit"), "a commit record"))
            found.append(CommittedOp(OpRef(str(op.get("opId")), _int(op, "attempt")), _int(op, "token"), commit))
        return found

    def rolled_back(self, op_id: str, attempt: int, token: int) -> None:
        self._call("POST", f"/operations/{self._id(op_id)}/rolled-back", {"token": token, "attempt": attempt})

    def publish(self, request: PublishRequest, lease: Lease) -> int:
        body = {
            "token": lease.token,
            "opId": request.op_id,
            "attempt": request.attempt,
            "commit": _commit_body(request.commit),
            "commitToken": request.commit_token,
        }
        data = _mapping(self._call("POST", f"/datasets/{self._id(lease.dataset_id)}/publish", body), "a revision")
        return _int(data, "revision")
