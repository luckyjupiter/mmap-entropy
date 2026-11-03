"""Ring writer: publishes byte chunks into free slots."""

from __future__ import annotations

import os
import threading
import time
import zlib
from pathlib import Path
from typing import Any

from .errors import WriterBusyError
from .layout import (
    SLOT_VERSION,
    RingFile,
    SlotHeader,
    create_ring,
)
from .sources import Source, UrandomSource


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class RingWriter:
    """Single-writer publisher for one ring.

    The writer resets stale claims on open and records its pid in the file
    header. It does not fsync; data lives in the page cache until the kernel
    writes it back.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        source: Source | None = None,
        force: bool = False,
    ) -> None:
        self.ring = RingFile(path)
        owner = self.ring.header.writer_pid
        if owner and owner != os.getpid() and _pid_alive(owner) and not force:
            self.ring.close()
            raise WriterBusyError(f"ring is owned by live writer pid {owner}")
        self.ring.reset_claims()
        self.ring.set_writer_pid(os.getpid())
        self.source = source or UrandomSource()
        self._sequence = self.ring.max_sequence()
        self._next_slot = 0
        for index in range(self.ring.header.slot_count):
            header = self.ring.read_slot_header(index)
            if (
                header is not None
                and self._sequence > 0
                and header.sequence == self._sequence
            ):
                self._next_slot = (index + 1) % self.ring.header.slot_count
                break
        self._publish_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.published = 0
        self.dropped = 0
        self.errors = 0

    @classmethod
    def create(
        cls,
        path: str | Path,
        *,
        source: Source | None = None,
        slots: int = 8,
        slot_size: int = 64 * 1024,
        overwrite: bool = False,
        force: bool = False,
    ) -> "RingWriter":
        create_ring(path, slots=slots, slot_size=slot_size, overwrite=overwrite)
        return cls(path, source=source, force=force)

    def _reserve_slot(self) -> int | None:
        with self.ring.lock():
            for offset in range(self.ring.header.slot_count):
                index = (self._next_slot + offset) % self.ring.header.slot_count
                if self.ring.claim_state(index) == 0:
                    self.ring.set_claim(index, 2)
                    self._next_slot = (index + 1) % self.ring.header.slot_count
                    return index
        return None

    def publish(self, data: bytes, *, timestamp_ns: int | None = None) -> int | None:
        """Write one chunk. Returns the slot index, or None if every slot is claimed."""
        payload = bytes(data)
        if not payload:
            return None
        if len(payload) > self.ring.header.slot_size:
            raise ValueError(
                f"payload is {len(payload)} bytes; slot capacity is "
                f"{self.ring.header.slot_size}"
            )
        with self._publish_lock:
            slot = self._reserve_slot()
            if slot is None:
                self.dropped += 1
                return None
            try:
                sequence = self._sequence + 1
                header = SlotHeader(
                    version=SLOT_VERSION,
                    flags=0,
                    capacity=self.ring.header.slot_size,
                    sequence=sequence,
                    timestamp_ns=timestamp_ns or time.time_ns(),
                    data_len=len(payload),
                    crc32=zlib.crc32(payload) & 0xFFFFFFFF,
                )
                self.ring.write_slot_header(slot, header)
                self.ring.write_data(slot, payload)
                self._sequence = sequence
                self.published += 1
                return slot
            finally:
                with self.ring.lock():
                    self.ring.set_claim(slot, 0)

    def start(self, *, interval: float = 0.05) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, args=(max(0.0, interval),), daemon=True
        )
        self._thread.start()

    def _run(self, interval: float) -> None:
        while not self._stop.is_set():
            try:
                data = self.source.read(self.ring.header.slot_size)
                if data:
                    self.publish(data)
            except Exception:  # noqa: BLE001 - keep the producer alive
                self.errors += 1
            self._stop.wait(interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def stats(self) -> dict[str, Any]:
        return {
            "path": str(self.ring.path),
            "slots": self.ring.header.slot_count,
            "slot_size": self.ring.header.slot_size,
            "sequence": self._sequence,
            "published": self.published,
            "dropped": self.dropped,
            "errors": self.errors,
            "running": bool(self._thread and self._thread.is_alive()),
        }

    def close(self) -> None:
        self.stop()
        # Leave reader claims alone; readers release them. A future writer
        # resets any stale claims when it takes ownership.
        self.ring.set_writer_pid(0)
        self.ring.close()

    def __enter__(self) -> "RingWriter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
