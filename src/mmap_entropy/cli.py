from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from .errors import MmapEntropyError
from .layout import create_ring, open_ring_header
from .reader import RingReader
from .sources import make_source
from .writer import RingWriter


def _human_bytes(value: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f}{unit}" if unit != "B" else f"{value}{unit}"
        value /= 1024
    return f"{value:.1f}GB"


def _cmd_create(args: argparse.Namespace) -> int:
    header = create_ring(
        args.path,
        slots=args.slots,
        slot_size=args.slot_size,
        overwrite=args.overwrite,
    )
    print(
        f"created {args.path}: {header.slot_count} slots × "
        f"{_human_bytes(header.slot_size)} = {_human_bytes(header.file_size)}"
    )
    return 0


def _cmd_write(args: argparse.Namespace) -> int:
    source = make_source(args.source, seed=args.seed)
    try:
        writer = RingWriter(args.path, source=source, force=args.force)
    except (OSError, ValueError, MmapEntropyError) as exc:
        print(f"mmap-entropy: {exc}", file=sys.stderr)
        return 2

    count = 0
    try:
        while args.count == 0 or count < args.count:
            data = source.read(writer.ring.header.slot_size)
            if data:
                writer.publish(data)
            count += 1
            if args.interval:
                time.sleep(max(0.0, args.interval))
    except KeyboardInterrupt:
        pass
    finally:
        stats = writer.stats()
        writer.close()
    if args.json:
        print(json.dumps(stats, indent=2))
    else:
        print(
            f"published={stats['published']} dropped={stats['dropped']} "
            f"errors={stats['errors']} sequence={stats['sequence']}"
        )
    return 0


def _cmd_read(args: argparse.Namespace) -> int:
    try:
        reader = RingReader(
            args.path,
            max_age_ms=args.max_age_ms,
            verify_crc=not args.no_verify,
        )
    except (OSError, ValueError, MmapEntropyError) as exc:
        print(f"mmap-entropy: {exc}", file=sys.stderr)
        return 2
    try:
        payload = reader.read(timeout=args.timeout)
        stats = reader.status()["stats"]
    finally:
        reader.close()
    if payload is None:
        print("(no fresh data)", file=sys.stderr)
        return 1
    if args.bytes:
        payload = payload[: args.bytes]
    if args.raw:
        sys.stdout.buffer.write(payload)
        sys.stdout.buffer.flush()
    else:
        print(payload.hex())
    if args.stats:
        print(json.dumps(stats), file=sys.stderr)
    return 0


def _cmd_stat(args: argparse.Namespace) -> int:
    try:
        reader = RingReader(args.path)
    except (OSError, ValueError, MmapEntropyError) as exc:
        print(f"mmap-entropy: {exc}", file=sys.stderr)
        return 2
    try:
        status = reader.status()
    finally:
        reader.close()
    if args.json:
        print(json.dumps(status, indent=2))
        return 0
    print(f"path:      {status['path']}")
    print(f"slots:     {status['slots']}")
    print(f"slot size: {_human_bytes(status['slot_size'])}")
    print(f"writer:    pid {status['writer_pid'] or '(none)'}")
    for slot in status["slot_status"]:
        age = f"{slot.get('age_ms')}ms" if slot.get("age_ms") is not None else "-"
        print(
            f"  slot {slot['index']:>3}  claim={slot['claim']}  "
            f"seq={slot.get('sequence', '-'):>8}  age={age:>10}"
        )
    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    rows = []
    for path in RingReader.discover(args.directory):
        try:
            header = open_ring_header(path)
        except Exception:  # noqa: BLE001
            continue
        rows.append(
            {
                "path": str(path),
                "slots": header.slot_count,
                "slot_size": header.slot_size,
                "writer_pid": header.writer_pid,
                "created_ns": header.created_ns,
            }
        )
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        print("(no rings found)")
        return 0
    for row in rows:
        print(
            f"  {row['path']}  slots={row['slots']}  "
            f"slot={_human_bytes(row['slot_size'])}  "
            f"writer={row['writer_pid'] or '-'}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="mmap-entropy",
        description="A fixed-layout mmap ring for local byte-chunk transport.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="create a ring file")
    create.add_argument("path")
    create.add_argument("--slots", type=int, default=8)
    create.add_argument("--slot-size", type=int, default=64 * 1024)
    create.add_argument("--overwrite", action="store_true")
    create.set_defaults(func=_cmd_create)

    write = sub.add_parser("write", help="publish chunks into a ring")
    write.add_argument("path")
    write.add_argument("--source", choices=["synthetic", "urandom"], default="synthetic")
    write.add_argument("--seed", type=int, default=0)
    write.add_argument("--interval", type=float, default=0.05)
    write.add_argument("--count", type=int, default=0, help="0 means run until interrupted")
    write.add_argument("--force", action="store_true", help="take over a live writer")
    write.add_argument("--json", action="store_true")
    write.set_defaults(func=_cmd_write)

    read = sub.add_parser("read", help="read one fresh chunk")
    read.add_argument("path")
    read.add_argument("--timeout", type=float, default=1.0)
    read.add_argument("--max-age-ms", type=int, default=1000)
    read.add_argument("--bytes", type=int, default=0, help="truncate output")
    read.add_argument("--raw", action="store_true", help="write raw bytes")
    read.add_argument("--no-verify", action="store_true", help="skip CRC check")
    read.add_argument("--stats", action="store_true", help="print stats to stderr")
    read.set_defaults(func=_cmd_read)

    stat = sub.add_parser("stat", help="show ring and slot status")
    stat.add_argument("path")
    stat.add_argument("--json", action="store_true")
    stat.set_defaults(func=_cmd_stat)

    listing = sub.add_parser("list", help="list rings in a directory")
    listing.add_argument("directory")
    listing.add_argument("--json", action="store_true")
    listing.set_defaults(func=_cmd_list)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (OSError, ValueError, MmapEntropyError) as exc:
        print(f"mmap-entropy: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
