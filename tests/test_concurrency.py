from __future__ import annotations

import threading

from mmap_entropy import RingReader, RingWriter, SyntheticSource


def test_single_reader_under_continuous_writes(tmp_path):
    writer = RingWriter.create(
        tmp_path / "stress.ring",
        source=SyntheticSource(3),
        slots=8,
        slot_size=1024,
    )
    try:
        writer.start(interval=0)
        with RingReader(tmp_path / "stress.ring", max_age_ms=2000) as reader:
            seen = 0
            for _ in range(200):
                payload = reader.read(timeout=2)
                assert payload is not None
                assert len(payload) == 1024
                seen += 1
            assert seen == 200
            assert reader.corrupt == 0
    finally:
        writer.close()


def test_multiple_readers_do_not_share_claims(tmp_path):
    writer = RingWriter.create(
        tmp_path / "multi.ring",
        source=SyntheticSource(4),
        slots=8,
        slot_size=512,
    )
    errors = []
    counts = []

    def consume() -> None:
        try:
            with RingReader(tmp_path / "multi.ring", max_age_ms=2000) as reader:
                local = 0
                for _ in range(50):
                    payload = reader.read(timeout=3)
                    assert payload is not None
                    assert len(payload) == 512
                    local += 1
                counts.append(local)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    try:
        writer.start(interval=0)
        threads = [threading.Thread(target=consume) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
    finally:
        writer.close()

    assert not errors
    assert sorted(counts) == [50, 50, 50, 50]
