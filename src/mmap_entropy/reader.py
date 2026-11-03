"""Ring reader: claims fresh slots, copies data, releases."""

from __future__ import annotations

import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import CorruptSlotError
from .layout import RingFile, SlotHeader, open_ring_header

_DEFAULT = object()


class Slot:
    """A claimed slot. Release is automatic when used as a context manager."""

    def __init__(self, ring: RingFile, index: int, header: SlotHeader) -> None:
        self.ring = ring
        self.index = index
        self.header = header
        self._released = False

    def data(self, *, verify_crc: bool = True) -> bytes:
        payload = self.ring.read_data(self.index, self.header.data_len)
        if len(payload) != self.header.data_len:
            raise CorruptSlotError("short read from slot")
        if verify_crc:
            expected = zlib.crc32(payload) & 0xFFFFFFFF
            if expected != self.header.crc32:
                raise CorruptSlotError("slot CRC mismatch")
        return payload

    def release(self) -> None:
        if self._released:
            return
        with self.ring.lock():
            self.ring.set_claim(self.index, 0)
        self._released = True

    def __enter__(self) -> "Slot":
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class RingReader:
    """Reads fresh chunks from a ring. One ring, one reader instance."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_age_ms: int | None = 1000,
        verify_crc: bool = True,
    ) -> None:
        self.ring = RingFile(path)
        self.max_age_ms = max_age_ms
        self.verify_crc = verify_crc
        self._next_slot = 0
        self._last_sequence = 0
        self.reads = 0
        self.stale_skips = 0
        self.empty_polls = 0
        self.corrupt = 0

    @classmethod
    def discover(cls, directory: str | Path) -> list[Path]:
        """Return valid ``*.ring`` files in a directory, sorted by name."""
        found: list[Path] = []
        for path in sorted(Path(directory).glob("*.ring")):
            try:
                open_ring_header(path)
            except Exception:  # noqa: BLE001 - ignore foreign or broken files
                continue
            found.append(path)
        return found

    def claim(
        self,
        *,
        timeout: float = 0.0,
        max_age_ms: int | None | object = _DEFAULT,
    ) -> Slot | None:
        """Claim one fresh, unclaimed slot. ``timeout`` of 0 makes one pass."""
        age_ms = self.max_age_ms if max_age_ms is _DEFAULT else max_age_ms
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            now_ns = time.time_ns()
            with self.ring.lock():
                best_index: int | None = None
                best_header: SlotHeader | None = None
                for index in range(self.ring.header.slot_count):
                    if self.ring.claim_state(index) != 0:
                        continue
                    header = self.ring.read_slot_header(index)
                    if header is None or not header.valid:
                        continue
                    if header.sequence <= self._last_sequence:
                        continue
                    if header.timestamp_ns > now_ns + 1_000_000_000:
                        continue
                    if age_ms is not None:
                        age_ns = now_ns - header.timestamp_ns
                        if age_ns > int(age_ms) * 1_000_000:
                            self.stale_skips += 1
                            continue
                    if best_header is None or header.sequence < best_header.sequence:
                        best_index = index
                        best_header = header
                if best_header is not None and best_index is not None:
                    self.ring.set_claim(best_index, 1)
                    self._last_sequence = best_header.sequence
                    self._next_slot = (best_index + 1) % self.ring.header.slot_count
                    return Slot(self.ring, best_index, best_header)
            if time.monotonic() >= deadline:
                self.empty_polls += 1
                return None
            time.sleep(min(0.005, max(0.0, deadline - time.monotonic())))

    def read(
        self,
        *,
        timeout: float = 0.0,
        max_age_ms: int | None | object = _DEFAULT,
    ) -> bytes | None:
        """Read one chunk, or None if no fresh slot is available."""
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            remaining = max(0.0, deadline - time.monotonic())
            slot = self.claim(timeout=remaining, max_age_ms=max_age_ms)
            if slot is None:
                return None
            with slot:
                try:
                    payload = slot.data(verify_crc=self.verify_crc)
                except CorruptSlotError:
                    self.corrupt += 1
                    if time.monotonic() >= deadline:
                        return None
                    continue
            self.reads += 1
            return payload

    def status(self) -> dict[str, Any]:
        now_ns = time.time_ns()
        slots = []
        for index in range(self.ring.header.slot_count):
            header = self.ring.read_slot_header(index)
            claim = self.ring.claim_state(index)
            if header is None:
                slots.append({"index": index, "claim": claim, "valid": False})
                continue
            age_ms = (
                (now_ns - header.timestamp_ns) / 1_000_000
                if header.timestamp_ns
                else None
            )
            slots.append(
                {
                    "index": index,
                    "claim": claim,
                    "valid": header.valid,
                    "sequence": header.sequence,
                    "data_len": header.data_len,
                    "age_ms": round(age_ms, 3) if age_ms is not None else None,
                }
            )
        return {
            "path": str(self.ring.path),
            "slots": self.ring.header.slot_count,
            "slot_size": self.ring.header.slot_size,
            "writer_pid": self.ring.header.writer_pid,
            "created_ns": self.ring.header.created_ns,
            "next_slot": self._next_slot,
            "last_sequence": self._last_sequence,
            "slot_status": slots,
            "stats": {
                "reads": self.reads,
                "stale_skips": self.stale_skips,
                "empty_polls": self.empty_polls,
                "corrupt": self.corrupt,
            },
        }

    def close(self) -> None:
        self.ring.close()

    def __enter__(self) -> "RingReader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
