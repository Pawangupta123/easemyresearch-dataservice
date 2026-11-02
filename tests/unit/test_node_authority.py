"""The writer's HTTP client to Node: signatures Node accepts, Node's answers read back, and each
refusal mapped to what the writer does with it — against a local HTTP server standing in for Node."""

import json
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from dataservice.common.errors import FenceError, OperationFailedError
from dataservice.modules.writer.node_authority import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    NodeAuthority,
    NodeRejected,
    NodeUnavailable,
    sign,
)
from dataservice.storage.fenced_commit import OpRef
from dataservice.storage.lease import Lease
from dataservice.storage.operations import Commit, JobResult, PublishRequest

SECRET = "s" * 32
PREFIX = "/api/v2/internal/dataservice"
DATASET = "5eed000000000000000f4240"
OP = "a" * 32
NOW = 1_700_000_000.0
COMMIT = Commit(base_revision=3, table_versions={"base": 7}, result=JobResult(rows_added=2, changed_field_ids=("f1",)))

Answer = tuple[int, object]


class FakeNode:
    """Answers each `(method, path)` with a canned (status, body); records what it was sent."""

    def __init__(self) -> None:
        self.answers: dict[tuple[str, str], Answer] = {}
        self.seen: list[dict[str, object]] = []


@pytest.fixture
def node() -> Iterator[tuple[FakeNode, NodeAuthority]]:
    fake = FakeNode()

    class Handler(BaseHTTPRequestHandler):
        def _answer(self) -> None:
            body = self.rfile.read(int(self.headers.get("content-length") or 0))
            fake.seen.append(
                {
                    "method": self.command,
                    "path": self.path,
                    "body": body,
                    "timestamp": self.headers.get(TIMESTAMP_HEADER),
                    "signature": self.headers.get(SIGNATURE_HEADER),
                }
            )
            status, reply = fake.answers.get((self.command, self.path), (404, {"success": False}))
            raw = json.dumps(reply).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        do_GET = do_POST = _answer

        def log_message(self, *_: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}{PREFIX}"
    yield fake, NodeAuthority(url, SECRET, timeout_seconds=5, clock=lambda: NOW)
    server.shutdown()
    server.server_close()


def ok(data: object) -> Answer:
    return 200, {"success": True, "message": "ok", "data": data}


def refused(status: int, code: str) -> Answer:
    return status, {"success": False, "message": "Something went wrong. Please try again.", "data": {"code": code}}


def test_the_signature_is_the_one_node_computes() -> None:
    # The same vector is pinned in the backend (tests/unit/common/dataservice-signature.test.ts).
    path = f"{PREFIX}/operations/{OP}/running"
    expected = "7e058d1ae1b11bf188f34497d5d30d84e82fbebf1a68d39440bf34bf1e1f3cb2"
    assert sign(SECRET, "1700000000000", "POST", path, b'{"token":3}') == expected


def test_every_call_is_signed_over_its_path_query_and_exact_body(node: tuple[FakeNode, NodeAuthority]) -> None:
    fake, authority = node
    fake.answers[("POST", f"{PREFIX}/operations/{OP}/running")] = ok({"attempt": 2})
    fake.answers[("GET", f"{PREFIX}/datasets/{DATASET}/operations/next?token=4")] = ok(None)
    assert authority.running(OP, 4) == 2
    assert authority.next_operation(DATASET, 4) is None
    for call in fake.seen:
        body = call["body"]
        assert isinstance(body, bytes)
        assert call["timestamp"] == "1700000000000"
        assert call["signature"] == sign(SECRET, "1700000000000", str(call["method"]), str(call["path"]), body)
    assert fake.seen[0]["body"] == b'{"token":4}'
    assert fake.seen[1]["body"] == b""


def test_the_lease_is_read_back_and_none_while_another_holds_it(node: tuple[FakeNode, NodeAuthority]) -> None:
    fake, authority = node
    path = ("POST", f"{PREFIX}/datasets/{DATASET}/lease")
    fake.answers[path] = ok({"token": 5, "expiresAt": "2026-11-02T10:00:00.000Z"})
    lease = authority.acquire(DATASET, "writer-a", 3)
    assert lease == Lease(DATASET, "writer-a", 5, datetime(2026, 11, 2, 10, tzinfo=UTC))
    assert json.loads(fake.seen[-1]["body"]) == {"action": "acquire", "owner": "writer-a", "maxTokenSeen": 3}  # type: ignore[arg-type]
    fake.answers[path] = ok(None)
    assert authority.renew(lease) is None


def test_committed_operations_come_back_as_the_writer_publishes_them(node: tuple[FakeNode, NodeAuthority]) -> None:
    fake, authority = node
    record = {
        "baseRevision": 3,
        "tableVersions": {"base": 7},
        "result": {"rowsAdded": 2, "rowsChanged": 0, "changedFieldIds": ["f1"], "touchedFieldIds": []},
    }
    fake.answers[("GET", f"{PREFIX}/datasets/{DATASET}/operations/committed")] = ok(
        [{"opId": OP, "attempt": 1, "token": 4, "commit": record}]
    )
    [op] = authority.committed_operations(DATASET)
    assert (op.ref, op.token, op.commit) == (OpRef(OP, 1), 4, COMMIT)


def test_publish_sends_the_commit_and_the_roll_forward_token(node: tuple[FakeNode, NodeAuthority]) -> None:
    fake, authority = node
    fake.answers[("POST", f"{PREFIX}/datasets/{DATASET}/publish")] = ok({"revision": 4})
    lease = Lease(DATASET, "writer-a", 6, datetime(2026, 11, 2, tzinfo=UTC))
    assert authority.publish(PublishRequest(OP, 1, COMMIT, commit_token=4), lease) == 4
    sent = json.loads(fake.seen[-1]["body"])  # type: ignore[arg-type]
    assert sent["token"] == 6 and sent["commitToken"] == 4 and sent["commit"]["result"]["rowsAdded"] == 2


@pytest.mark.parametrize(
    ("status", "code", "raised"),
    [(409, "FENCED", FenceError), (409, "OPERATION_FAILED", OperationFailedError), (409, "CONFLICT", NodeRejected)],
)
def test_each_refusal_is_what_the_writer_acts_on(
    node: tuple[FakeNode, NodeAuthority], status: int, code: str, raised: type[Exception]
) -> None:
    fake, authority = node
    fake.answers[("POST", f"{PREFIX}/operations/{OP}/running")] = refused(status, code)
    with pytest.raises(raised):
        authority.running(OP, 4)


def test_an_unreachable_node_is_retryable_not_a_refusal() -> None:
    authority = NodeAuthority("http://127.0.0.1:9/api", SECRET, timeout_seconds=1)
    with pytest.raises(NodeUnavailable):
        authority.manifest(DATASET)
