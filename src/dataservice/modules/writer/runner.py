"""Draining a dataset in the background (consistency §4.4): take its lease, keep it renewed,
recover + run its queue (service.drain), give the lease back.

Node wakes a dataset with `writer:drain`; the call answers at once and the drain runs on a
thread. One drain per dataset per process: a wake-up that arrives while one runs is remembered
and the queue is drained once more after it — so an operation queued during a drain is never
left waiting for the sweeper. Across processes, the lease lets only one writer run.
"""

import os
import socket
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

import structlog
from deltalake import DeltaTable

from dataservice.common.errors import FenceError
from dataservice.modules.writer.service import WriterTables, drain
from dataservice.storage.fenced_commit import max_token_seen
from dataservice.storage.layout import dataset_table_uris
from dataservice.storage.lease import Lease, LeaseAuthority
from dataservice.storage.operations import OperationAuthority

log = structlog.get_logger()


class Authority(LeaseAuthority, OperationAuthority, Protocol):
    """Node's side: the lease and the operation queue."""


def writer_owner() -> str:
    """This process as a lease owner: unique per process and per start."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


@dataclass
class _Heartbeat:
    """Renews the lease until stopped; a lost lease is only logged — Node's door fences the
    writer's next call, which stops the drain."""

    authority: LeaseAuthority
    lease: Lease
    every_seconds: float
    stopped: threading.Event = field(default_factory=threading.Event)

    def run(self) -> None:
        while not self.stopped.wait(self.every_seconds):
            try:
                renewed = self.authority.renew(self.lease)
            except Exception:
                log.warning("writer_lease_renew_failed", dataset_id=self.lease.dataset_id, exc_info=True)
                continue
            if renewed is None:
                log.warning("writer_lease_lost", dataset_id=self.lease.dataset_id, token=self.lease.token)
                return
            self.lease = renewed


class DrainRunner:
    def __init__(
        self,
        authority: Authority,
        lake_root: str,
        renew_seconds: float,
        owner: str | None = None,
        start: Callable[[Callable[[], None]], None] | None = None,
    ) -> None:
        self._authority, self._lake_root, self._renew_seconds = authority, lake_root, renew_seconds
        self.owner = owner or writer_owner()
        self._start = start or (lambda work: threading.Thread(target=work, daemon=True).start())
        self._guard = threading.Lock()
        self._running: set[str] = set()
        self._again: set[str] = set()

    def request(self, dataset_id: str) -> bool:
        """Drain the dataset soon. True when a drain was started; False when one is running (it
        will drain again once it finishes)."""
        with self._guard:
            if dataset_id in self._running:
                self._again.add(dataset_id)
                return False
            self._running.add(dataset_id)
        self._start(lambda: self._loop(dataset_id))
        return True

    def _loop(self, dataset_id: str) -> None:
        try:
            while True:
                with self._guard:
                    self._again.discard(dataset_id)
                self.drain_once(dataset_id)
                with self._guard:
                    if dataset_id not in self._again:
                        self._running.discard(dataset_id)
                        return
        except BaseException:
            with self._guard:
                self._running.discard(dataset_id)
                self._again.discard(dataset_id)
            raise

    def drain_once(self, dataset_id: str) -> None:
        """One drain under the lease; every failure is logged, never raised (a background job)."""
        uris = dataset_table_uris(self._lake_root, dataset_id)
        existing = {name: uri for name, uri in uris.items() if DeltaTable.is_deltatable(uri)}
        if not existing:
            log.warning("writer_drain_no_tables", dataset_id=dataset_id)
            return
        tables = WriterTables(existing)
        try:
            lease = self._authority.acquire(dataset_id, self.owner, max_token_seen(tables.open(n) for n in existing))
        except Exception:
            log.error("writer_lease_acquire_failed", dataset_id=dataset_id, exc_info=True)
            return
        if lease is None:
            log.info("writer_drain_skipped_lease_held", dataset_id=dataset_id)
            return
        heartbeat = _Heartbeat(self._authority, lease, self._renew_seconds)
        threading.Thread(target=heartbeat.run, daemon=True).start()
        try:
            result = drain(dataset_id, lease, tables, self._authority)
            log.info(
                "writer_drained",
                dataset_id=dataset_id,
                token=lease.token,
                applied=result.applied,
                failed=result.failed,
                rolled_forward=result.recovery.rolled_forward,
            )
        except FenceError:
            log.warning("writer_fenced", dataset_id=dataset_id, token=lease.token)
        except Exception:
            log.error("writer_drain_failed", dataset_id=dataset_id, token=lease.token, exc_info=True)
        finally:
            heartbeat.stopped.set()
            try:
                self._authority.release(heartbeat.lease)
            except Exception:
                log.warning("writer_lease_release_failed", dataset_id=dataset_id, exc_info=True)
