from __future__ import annotations

import pytest

from mmap_entropy.layout import (
    FILE_HEADER_SIZE,
    create_ring,
    lock_path,
    open_ring_header,
)
from mmap_entropy.errors import RingFormatError


def test_create_and_open(tmp_path):
    path = tmp_path / "test.ring"
    header = create_ring(path, slots=4, slot_size=1024)
    assert header.slot_count == 4
    assert header.slot_size == 1024
    assert path.stat().st_size == header.file_size
    assert lock_path(path).exists()

    reopened = open_ring_header(path)
    assert reopened == header
    assert reopened.claims_offset == FILE_HEADER_SIZE


def test_refuses_to_overwrite(tmp_path):
    path = tmp_path / "test.ring"
    create_ring(path, slots=2, slot_size=128)
    with pytest.raises(FileExistsError):
        create_ring(path, slots=2, slot_size=128)
    create_ring(path, slots=2, slot_size=128, overwrite=True)


def test_rejects_truncated_or_foreign_file(tmp_path):
    path = tmp_path / "short.ring"
    path.write_bytes(b"not a ring")
    with pytest.raises(RingFormatError):
        open_ring_header(path)

    foreign = tmp_path / "foreign.ring"
    foreign.write_bytes(b"X" * 256)
    with pytest.raises(RingFormatError):
        open_ring_header(foreign)


def test_rejects_invalid_geometry(tmp_path):
    with pytest.raises(ValueError):
        create_ring(tmp_path / "a.ring", slots=0)
    with pytest.raises(ValueError):
        create_ring(tmp_path / "b.ring", slot_size=0)
