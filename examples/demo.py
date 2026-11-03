#!/usr/bin/env python3
"""Offline demo: one writer, two readers, and two rings in a temp directory.

No hardware, service, or network is involved. The writer uses a deterministic
synthetic source so the payload prefix is reproducible.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

from mmap_entropy import RingReader, RingWriter, SyntheticSource


def show(reader: RingReader, label: str, timeout: float = 2.0) -> None:
    with reader.claim(timeout=timeout) as slot:
        payload = slot.data()
        print(
            f"{label}: seq={slot.header.sequence} bytes={len(payload)} "
            f"prefix={payload[:4].hex()}"
        )


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        stream = RingWriter.create(
            root / "stream.ring",
            source=SyntheticSource(seed=7),
            slots=8,
            slot_size=4096,
        )
        stream.start(interval=0.01)
        try:
            with RingReader(root / "stream.ring") as reader:
                for number in range(3):
                    show(reader, f"reader {number + 1}")
        finally:
            stream.stop()
            print(
                f"writer: published={stream.published} "
                f"dropped={stream.dropped} sequence={stream.stats()['sequence']}"
            )
            stream.close()

        fanout = RingWriter.create(
            root / "fanout.ring",
            source=SyntheticSource(seed=1),
            slots=4,
            slot_size=4096,
        )
        try:
            fanout.publish(b"fanout-chunk-1")
            fanout.publish(b"fanout-chunk-2")
        finally:
            fanout.close()

        with RingReader(root / "fanout.ring") as first, RingReader(
            root / "fanout.ring"
        ) as second:
            for label, reader in (("first", first), ("second", second)):
                values = [reader.read(timeout=1), reader.read(timeout=1)]
                print(f"{label}: {values}")

        print(
            "discovered:",
            ", ".join(path.name for path in RingReader.discover(root)),
        )


if __name__ == "__main__":
    main()
