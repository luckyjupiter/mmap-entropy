# Design notes

## The problem

Several local processes needed the most recent chunk of bytes from one producer.
A socket hop copies the data and can block a consumer behind a slow reader. A
normal file reread is also a copy, and a durable queue keeps data the consumer
does not want. The requirement was narrower:

- one producer, many independent consumers;
- small, fixed-size chunks;
- recent data only, with a bounded staleness window;
- an unclaimed slot must be stable while a consumer copies it;
- a consumer that falls behind should miss old data rather than slow the
  producer;
- no service, daemon framework, or hardware dependency in the core.

## The decision

The transport is one memory-mapped file with a fixed layout and an advisory
lock:

1. The file has `N` slots of `slot_size` bytes plus a small header, a claim byte
   per slot, and a CRC32 in each slot header.
2. The writer is single. It records its pid, resets stale claims, and publishes
   in round-robin order. It skips a claimed slot and drops the chunk if every
   slot is claimed.
3. A reader takes the lock, finds the lowest fresh sequence greater than its own
   cursor, marks the slot `1`, unlocks, copies the bytes, verifies the CRC, and
   marks the slot `0`.
4. The writer marks a slot `2` while writing, so a reader never copies a
   half-written slot. The lock is only held for state transitions and header
   scanning; the payload copy is outside it.
5. `max_age_ms` rejects stale slots. A slot stamped more than one second in the
   future is rejected.

The sequence cursor is per reader. It is what makes two readers receive the same
published chunk once, without a shared cursor that would let one reader consume
data for the other.

## Rejected alternatives

- **Socket or pipe per consumer.** Extra copies, backpressure on the producer,
  and a connection lifecycle to manage.
- **One shared read cursor.** Readers interfere with each other and a slow
  reader creates head-of-line blocking.
- **A fully lock-free ring.** Portable correctness around slot reuse and torn
  reads is harder to explain and test; the advisory lock is adequate for a
  trusted local group.
- **A durable queue.** Wrong semantics. This transport intentionally drops old
  data and does not fsync.
- **Putting the claim map in a separate file.** It would allow read-only data
  mappings, but adds a second file and a create/cleanup race for little benefit
  at this scale. Readers open read-write; this is documented.

## Executable example

```sh
python -m pip install -e '.[dev]'
python examples/demo.py
```

The demo starts one writer and two readers over a temporary ring, then shows a
second ring with two independent readers. It uses a deterministic synthetic
source; no hardware or network is involved.

CLI:

```sh
mmap-entropy create /dev/shm/demo.ring --slots 8 --slot-size 65536
mmap-entropy write /dev/shm/demo.ring --source synthetic --count 5 --interval 0
mmap-entropy read /dev/shm/demo.ring --timeout 1 --bytes 16
mmap-entropy stat /dev/shm/demo.ring
```

## Verification

The test suite covers:

- create/open validation, rejected truncated and foreign files;
- round-trips and payload limits;
- claim exclusivity and writer skipping a claimed slot;
- dropping when all slots are claimed;
- stale rejection and CRC corruption;
- stale claim recovery and live-writer refusal;
- multi-ring discovery;
- CLI create/write/read/stat/list;
- a concurrent writer with one reader and with four readers.

CI runs the suite on Linux Python 3.10, 3.12, and 3.13, and on macOS Python
3.12. No latency or throughput benchmark is claimed; measure the target
filesystem and payload size.

## Limits

- POSIX only (`fcntl`); Windows is out of scope.
- The ring is a trusted-local-user object. `flock` is advisory and readers open
  read-write because claims live in the mapping.
- The ring is lossy: a full ring drops writes; a slow reader skips sequences the
  writer has lapped.
- No `fsync`, acknowledgement, retry, exactly-once delivery, encryption, or
  network transport.
- A writer takeover resets claims, including claims held by live readers.
  Quiesce readers before replacing a writer.

The exact byte layout and state machine are in
[FORMAT.md](FORMAT.md).
