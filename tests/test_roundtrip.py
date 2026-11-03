from __future__ import annotations

import time

import pytest

from mmap_entropy import RingReader, RingWriter, SyntheticSource
from mmap_entropy.errors import CorruptSlotError


def make_writer(tmp_path, *, slots=4, slot_size=1024, seed=1):
    return RingWriter.create(
        tmp_path / "test.ring",
        source=SyntheticSource(seed),
        slots=slots,
        slot_size=slot_size,
    )


def test_roundtrip_and_order(tmp_path):
    writer = make_writer(tmp_path)
    try:
        assert writer.publish(b"one") == 0
        assert writer.publish(b"two") == 1
        assert writer.publish(b"three") == 2
        with RingReader(tmp_path / "test.ring") as reader:
            assert reader.read() == b"one"
            assert reader.read() == b"two"
            assert reader.read() == b"three"
            assert reader.read(timeout=0) is None
    finally:
        writer.close()


def test_payload_limit_and_empty(tmp_path):
    writer = make_writer(tmp_path, slot_size=4)
    try:
        assert writer.publish(b"") is None
        with pytest.raises(ValueError):
            writer.publish(b"too large")
        writer.publish(b"abcd")
        with RingReader(tmp_path / "test.ring") as reader:
            assert reader.read() == b"abcd"
    finally:
        writer.close()


def test_stale_slots_are_rejected(tmp_path):
    writer = make_writer(tmp_path)
    try:
        writer.publish(b"stale")
        time.sleep(0.02)
        with RingReader(tmp_path / "test.ring", max_age_ms=5) as reader:
            assert reader.read() is None
            assert reader.stale_skips >= 1
        fresh = RingReader(tmp_path / "test.ring", max_age_ms=5000)
        try:
            assert fresh.read() == b"stale"
        finally:
            fresh.close()
    finally:
        writer.close()


def test_crc_corruption_is_detected(tmp_path):
    writer = make_writer(tmp_path)
    try:
        writer.publish(b"hello")
        with RingReader(tmp_path / "test.ring") as reader:
            with reader.claim() as slot:
                assert slot.data() == b"hello"
                slot.ring.write_data(slot.index, b"HELLO")
                with pytest.raises(CorruptSlotError):
                    slot.data()
            assert reader.read() is None
            with RingReader(tmp_path / "test.ring") as checker:
                assert checker.read() is None
                assert checker.corrupt == 1
    finally:
        writer.close()


def test_claim_is_exclusive(tmp_path):
    writer = make_writer(tmp_path, slots=2)
    try:
        writer.publish(b"only")
        with RingReader(tmp_path / "test.ring") as first:
            with first.claim() as slot:
                with RingReader(tmp_path / "test.ring") as second:
                    assert second.claim() is None
                assert slot.data() == b"only"
    finally:
        writer.close()


def test_writer_drops_when_every_slot_is_claimed(tmp_path):
    writer = make_writer(tmp_path, slots=1)
    try:
        writer.publish(b"first")
        with RingReader(tmp_path / "test.ring") as reader:
            with reader.claim() as slot:
                assert slot.data() == b"first"
                assert writer.publish(b"second") is None
                assert writer.dropped == 1
    finally:
        writer.close()


def test_writer_skips_claimed_slot(tmp_path):
    writer = make_writer(tmp_path, slots=2)
    try:
        writer.publish(b"one")
        writer.publish(b"two")
        with RingReader(tmp_path / "test.ring") as reader:
            with reader.claim() as slot:
                held_index = slot.index
                held_sequence = slot.header.sequence
                for _ in range(5):
                    writer.publish(b"new")
                assert writer.ring.read_slot_header(held_index).sequence == held_sequence
                other = 1 - held_index
                assert writer.ring.read_slot_header(other).sequence > 2
    finally:
        writer.close()


def test_writer_resets_stale_claims_on_open(tmp_path):
    writer = make_writer(tmp_path, slots=2)
    writer.publish(b"one")
    writer.close()

    from mmap_entropy.layout import RingFile

    ring = RingFile(tmp_path / "test.ring")
    try:
        with ring.lock():
            ring.set_claim(0, 2)
            ring.set_claim(1, 1)
    finally:
        ring.close()

    writer = RingWriter(
        tmp_path / "test.ring", source=SyntheticSource(2)
    )
    try:
        assert writer.ring.claim_state(0) == 0
        assert writer.ring.claim_state(1) == 0
        writer.publish(b"two")
    finally:
        writer.close()


def test_live_writer_is_refused_unless_forced(tmp_path):
    import os

    from mmap_entropy import WriterBusyError
    from mmap_entropy.layout import RingFile

    writer = make_writer(tmp_path)
    writer.close()
    ring = RingFile(tmp_path / "test.ring")
    try:
        ring.set_writer_pid(os.getppid())
    finally:
        ring.close()

    with pytest.raises(WriterBusyError):
        RingWriter(tmp_path / "test.ring", source=SyntheticSource(5))
    forced = RingWriter(
        tmp_path / "test.ring", source=SyntheticSource(5), force=True
    )
    forced.close()
