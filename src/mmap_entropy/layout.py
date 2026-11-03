"""On-disk layout and mmap access for a single ring."""

from __future__ import annotations

import mmap
import os
import struct
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .errors import RingFormatError

FILE_MAGIC = b"MMAPRING"
FILE_VERSION = 1
FILE_HEADER_SIZE = 64
FILE_HEADER = struct.Struct("<8sHHIIIIQQI16s")
# magic, version, flags, header_size, slot_count, slot_size, stride,
# created_ns, writer_pid, reserved, padding

SLOT_MAGIC = b"MMAPSLOT"
SLOT_VERSION = 1
SLOT_HEADER_SIZE = 44
SLOT_HEADER = struct.Struct("<8sHHIIQQII")
# magic, version, flags, header_size, capacity, sequence, timestamp_ns,
# data_len, crc32

MAX_SLOTS = 4096
MAX_SLOT_SIZE = 64 * 1024 * 1024
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024


def _align64(value: int) -> int:
    return (value + 63) & ~63


@dataclass(frozen=True)
class RingHeader:
    version: int
    flags: int
    slot_count: int
    slot_size: int
    stride: int
    created_ns: int
    writer_pid: int
    reserved: int

    @property
    def claims_offset(self) -> int:
        return FILE_HEADER_SIZE

    @property
    def slots_offset(self) -> int:
        return _align64(FILE_HEADER_SIZE + self.slot_count)

    @property
    def file_size(self) -> int:
        return self.slots_offset + self.slot_count * self.stride


@dataclass(frozen=True)
class SlotHeader:
    version: int
    flags: int
    capacity: int
    sequence: int
    timestamp_ns: int
    data_len: int
    crc32: int

    @property
    def valid(self) -> bool:
        return (
            self.version == SLOT_VERSION
            and self.capacity > 0
            and self.data_len <= self.capacity
            and self.sequence > 0
        )


def lock_path(path: str | Path) -> Path:
    path = Path(path)
    return path.with_name(path.name + ".lock")


def _pack_file_header(header: RingHeader) -> bytes:
    return FILE_HEADER.pack(
        FILE_MAGIC,
        header.version,
        header.flags,
        FILE_HEADER_SIZE,
        header.slot_count,
        header.slot_size,
        header.stride,
        header.created_ns,
        header.writer_pid,
        header.reserved,
        b"\0" * 16,
    )


def pack_slot_header(header: SlotHeader) -> bytes:
    return SLOT_HEADER.pack(
        SLOT_MAGIC,
        header.version,
        header.flags,
        SLOT_HEADER_SIZE,
        header.capacity,
        header.sequence,
        header.timestamp_ns,
        header.data_len,
        header.crc32,
    )


def unpack_slot_header(raw: bytes) -> SlotHeader | None:
    if len(raw) != SLOT_HEADER_SIZE:
        return None
    (
        magic,
        version,
        flags,
        header_size,
        capacity,
        sequence,
        timestamp_ns,
        data_len,
        crc32,
    ) = SLOT_HEADER.unpack(raw)
    if magic != SLOT_MAGIC or header_size != SLOT_HEADER_SIZE:
        return None
    return SlotHeader(
        version=version,
        flags=flags,
        capacity=capacity,
        sequence=sequence,
        timestamp_ns=timestamp_ns,
        data_len=data_len,
        crc32=crc32,
    )


def open_ring_header(path: str | Path) -> RingHeader:
    path = Path(path)
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            raw = handle.read(FILE_HEADER_SIZE)
    except OSError as exc:
        raise RingFormatError(f"cannot read ring {path}: {exc}") from exc
    if len(raw) != FILE_HEADER_SIZE:
        raise RingFormatError(f"{path}: truncated file header")
    (
        magic,
        version,
        flags,
        header_size,
        slot_count,
        slot_size,
        stride,
        created_ns,
        writer_pid,
        reserved,
        _padding,
    ) = FILE_HEADER.unpack(raw)
    if magic != FILE_MAGIC:
        raise RingFormatError(f"{path}: not a mmap-entropy ring")
    if version != FILE_VERSION:
        raise RingFormatError(f"{path}: unsupported version {version}")
    if header_size != FILE_HEADER_SIZE:
        raise RingFormatError(f"{path}: unsupported header size {header_size}")
    if not (1 <= slot_count <= MAX_SLOTS):
        raise RingFormatError(f"{path}: invalid slot count {slot_count}")
    if not (1 <= slot_size <= MAX_SLOT_SIZE):
        raise RingFormatError(f"{path}: invalid slot size {slot_size}")
    if stride < SLOT_HEADER_SIZE + slot_size:
        raise RingFormatError(f"{path}: invalid slot stride {stride}")
    header = RingHeader(
        version=version,
        flags=flags,
        slot_count=slot_count,
        slot_size=slot_size,
        stride=stride,
        created_ns=created_ns,
        writer_pid=writer_pid,
        reserved=reserved,
    )
    if size != header.file_size:
        raise RingFormatError(
            f"{path}: size {size} does not match layout {header.file_size}"
        )
    return header


def create_ring(
    path: str | Path,
    *,
    slots: int = 8,
    slot_size: int = 64 * 1024,
    overwrite: bool = False,
    mode: int = 0o660,
) -> RingHeader:
    """Create and initialize a ring file. This destroys an existing ring only
    when ``overwrite`` is true."""
    path = Path(path)
    if not (1 <= slots <= MAX_SLOTS):
        raise ValueError(f"slots must be between 1 and {MAX_SLOTS}")
    if not (1 <= slot_size <= MAX_SLOT_SIZE):
        raise ValueError(f"slot_size must be between 1 and {MAX_SLOT_SIZE}")
    stride = _align64(SLOT_HEADER_SIZE + slot_size)
    header = RingHeader(
        version=FILE_VERSION,
        flags=0,
        slot_count=slots,
        slot_size=slot_size,
        stride=stride,
        created_ns=time.time_ns(),
        writer_pid=0,
        reserved=0,
    )
    if header.file_size > MAX_FILE_SIZE:
        raise ValueError(f"ring would exceed {MAX_FILE_SIZE} bytes")
    if path.exists() and not overwrite:
        raise FileExistsError(f"ring already exists: {path}")

    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT | os.O_TRUNC
    fd = os.open(path, flags, mode)
    try:
        os.ftruncate(fd, header.file_size)
        with mmap.mmap(fd, header.file_size) as mapping:
            mapping[0:FILE_HEADER_SIZE] = _pack_file_header(header)
            empty = SlotHeader(
                version=SLOT_VERSION,
                flags=0,
                capacity=slot_size,
                sequence=0,
                timestamp_ns=0,
                data_len=0,
                crc32=0,
            )
            packed = pack_slot_header(empty)
            for index in range(slots):
                offset = header.slots_offset + index * header.stride
                mapping[offset : offset + SLOT_HEADER_SIZE] = packed
            mapping.flush()
    finally:
        os.close(fd)

    lock = lock_path(path)
    lock_fd = os.open(lock, os.O_RDWR | os.O_CREAT, mode)
    os.close(lock_fd)
    try:
        path.chmod(mode)
        lock.chmod(mode)
    except OSError:
        pass
    return header


class RingFile:
    """A mapped ring file. Claims are protected by an advisory ``flock``."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.header = open_ring_header(self.path)
        self._file = self.path.open("r+b", buffering=0)
        self._mapping = mmap.mmap(
            self._file.fileno(), 0, access=mmap.ACCESS_WRITE
        )
        self._lock_fd = os.open(lock_path(self.path), os.O_RDWR | os.O_CREAT, 0o660)

    def close(self) -> None:
        try:
            self._mapping.close()
        finally:
            self._file.close()
            os.close(self._lock_fd)

    @contextmanager
    def lock(self) -> Iterator[None]:
        import fcntl

        fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)

    def claim_state(self, index: int) -> int:
        return self._mapping[self.header.claims_offset + index]

    def set_claim(self, index: int, value: int) -> None:
        self._mapping[self.header.claims_offset + index] = value

    def reset_claims(self) -> None:
        with self.lock():
            start = self.header.claims_offset
            self._mapping[start : start + self.header.slot_count] = (
                b"\0" * self.header.slot_count
            )

    def set_writer_pid(self, pid: int) -> None:
        header = self.header
        updated = RingHeader(
            version=header.version,
            flags=header.flags,
            slot_count=header.slot_count,
            slot_size=header.slot_size,
            stride=header.stride,
            created_ns=header.created_ns,
            writer_pid=pid,
            reserved=header.reserved,
        )
        with self.lock():
            self._mapping[0:FILE_HEADER_SIZE] = _pack_file_header(updated)
        self.header = updated

    def slot_offset(self, index: int) -> int:
        if not (0 <= index < self.header.slot_count):
            raise IndexError(index)
        return self.header.slots_offset + index * self.header.stride

    def read_slot_header(self, index: int) -> SlotHeader | None:
        offset = self.slot_offset(index)
        return unpack_slot_header(
            self._mapping[offset : offset + SLOT_HEADER_SIZE]
        )

    def write_slot_header(self, index: int, header: SlotHeader) -> None:
        offset = self.slot_offset(index)
        self._mapping[offset : offset + SLOT_HEADER_SIZE] = pack_slot_header(header)

    def read_data(self, index: int, length: int) -> bytes:
        offset = self.slot_offset(index) + SLOT_HEADER_SIZE
        return bytes(self._mapping[offset : offset + length])

    def write_data(self, index: int, data: bytes) -> None:
        if len(data) > self.header.slot_size:
            raise ValueError("data larger than slot")
        offset = self.slot_offset(index) + SLOT_HEADER_SIZE
        self._mapping[offset : offset + len(data)] = data

    def max_sequence(self) -> int:
        sequences = []
        for index in range(self.header.slot_count):
            header = self.read_slot_header(index)
            if header is not None:
                sequences.append(header.sequence)
        return max(sequences, default=0)
