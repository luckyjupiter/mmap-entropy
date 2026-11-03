# mmap-entropy

A fixed-layout memory-mapped ring for passing byte chunks between processes on
the same host. A producer publishes into slots; readers claim a stable slot,
copy the bytes, and release it. No socket, daemon framework, database, or
hardware driver is involved.

The transport is intentionally small: one file, an advisory file lock, a CRC,
and a freshness check. It is useful when several local consumers need a recent
chunk of bytes without copying through a socket, and when dropping stale data is
preferable to blocking the producer.

Status: beta. Linux and macOS (it uses `fcntl`). Python 3.10+.

## Try it

```sh
python -m pip install -e '.[dev]'
python examples/demo.py
```

```text
reader 1: seq=1 bytes=4096 prefix=38b4e652
reader 2: seq=2 bytes=4096 prefix=b5006277
reader 3: seq=3 bytes=4096 prefix=a8ad5435
writer: published=3 dropped=0 sequence=3
first: [b'fanout-chunk-1', b'fanout-chunk-2']
second: [b'fanout-chunk-1', b'fanout-chunk-2']
discovered: fanout.ring, stream.ring
```

## CLI

```sh
mmap-entropy create /dev/shm/demo.ring --slots 8 --slot-size 65536
mmap-entropy write /dev/shm/demo.ring --source synthetic --count 5 --interval 0
mmap-entropy read /dev/shm/demo.ring --timeout 1 --bytes 16
mmap-entropy stat /dev/shm/demo.ring
mmap-entropy list /dev/shm
```

`write --count 0` runs until interrupted. `read --raw` writes bytes instead of
hex. `stat --json` and `list --json` are available for tooling.

## Python

```python
from mmap_entropy import RingReader, RingWriter, SyntheticSource

writer = RingWriter.create(
    "/dev/shm/demo.ring",
    source=SyntheticSource(seed=1),
    slots=8,
    slot_size=65536,
)
writer.publish(b"one chunk")
writer.publish(b"another chunk")

with RingReader("/dev/shm/demo.ring", max_age_ms=1000) as reader:
    while (chunk := reader.read(timeout=0.5)) is not None:
        print(len(chunk), chunk[:4].hex())

writer.close()
```

Two readers can read the same ring independently. Each keeps its own sequence
cursor, so it consumes each published chunk once:

```python
with RingReader(path) as first, RingReader(path) as second:
    a = first.read(timeout=1)
    b = second.read(timeout=1)
    assert a == b
```

## Semantics

- The file contains `slots` fixed-size slots. When all slots are claimed, the
  writer drops the chunk instead of waiting. When slots wrap, the oldest data is
  overwritten. This is a lossy ring, not a durable queue.
- A reader claims a slot under an advisory `flock`, copies the payload, and
  releases it. The writer never writes a slot in state `1` (reader) or `2`
  (writer), so a claimed slot is stable.
- Each reader tracks the highest sequence it has consumed and returns the
  lowest fresh sequence greater than that cursor. If the writer laps the reader,
  the reader skips the overwritten chunks. There is no acknowledgement, retry,
  or exactly-once delivery.
- `max_age_ms` rejects stale slots (default 1000 ms). `None` disables the check.
- Every slot carries a CRC32. `read()` skips a corrupt slot and counts it.
- The writer is single. A live writer pid in the file header blocks a second
  writer unless `force=True`; a stale pid is taken over. Starting a writer resets
  all claim bytes, so quiesce readers before replacing a live writer. `force=True`
  is a takeover and can disrupt a writer that is still running.
- There is no `fsync`. Data is visible through the shared mapping immediately,
  but the file is not a durability boundary.
- If a writer dies with a slot in state `2`, the next writer resets all claims
  on open. Readers cannot recover the slot themselves.

## Limits

- POSIX only. Windows has no `fcntl` here.
- The ring must be on a local filesystem. Do not use NFS or another network
  filesystem for shared-memory semantics.
- Readers open the ring read-write because the claim map lives in the same
  mapping. Treat the ring as a trusted-local-user object, not a security
  boundary.
- `flock` is advisory. A process that ignores the protocol can corrupt the ring.
- One writer per ring. Multiple writers are not coordinated.
- No benchmark is claimed here. Measure your own filesystem and payload sizes.

## Format

The exact file layout, structs, and state transitions are in
[docs/FORMAT.md](docs/FORMAT.md).

## Tests

```sh
python -m pip install -e '.[dev]'
python -m pytest
```

The suite uses temporary files and covers round-trips, claim exclusivity,
writer slot skipping, stale rejection, CRC corruption, cursor behavior, live
writer refusal, discovery, CLI flow, and a concurrent writer/multiple-reader
stress run.

## License

MIT. The design is a generic extraction from internal byte-transport tooling;
no research or statistical processing is included.
