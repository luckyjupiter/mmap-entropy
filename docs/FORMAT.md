# Ring format

All integers are little-endian. One ring is one regular file plus an advisory
lock file named `<ring>.lock`. The file is mapped read-write by the writer and
by readers (readers write claim bytes).

## Geometry

```text
file header (64 bytes)
claim bytes (slot_count bytes, one per slot)
padding to a 64-byte boundary
slot 0:  44-byte header + slot_size data + padding
slot 1:  ...
...
```

```text
claims_offset = 64
slots_offset  = align64(64 + slot_count)
stride        = align64(44 + slot_size)
file_size     = slots_offset + slot_count * stride
```

`align64(n)` rounds up to a multiple of 64. The padding bytes are not used.

## File header

`struct.Struct("<8sHHIIIIQQI16s")`, 64 bytes:

| Offset | Size | Field | Meaning |
| --- | --- | --- | --- |
| 0 | 8 | magic | `MMAPRING` |
| 8 | 2 | version | currently `1` |
| 10 | 2 | flags | reserved, `0` |
| 12 | 4 | header_size | `64` |
| 16 | 4 | slot_count | number of slots |
| 20 | 4 | slot_size | payload capacity per slot |
| 24 | 4 | stride | bytes per slot region |
| 28 | 8 | created_ns | creation time, Unix ns |
| 36 | 8 | writer_pid | current writer pid, `0` when none |
| 44 | 4 | reserved | `0` |
| 48 | 16 | padding | zero |

## Slot header

`struct.Struct("<8sHHIIQQII")`, 44 bytes:

| Offset | Size | Field | Meaning |
| --- | --- | --- | --- |
| 0 | 8 | magic | `MMAPSLOT` |
| 8 | 2 | version | currently `1` |
| 10 | 2 | flags | reserved, `0` |
| 12 | 4 | header_size | `44` |
| 16 | 4 | capacity | equals file `slot_size` |
| 20 | 8 | sequence | monotonically increasing per published chunk |
| 28 | 8 | timestamp_ns | publish time, Unix ns |
| 36 | 4 | data_len | payload length, `0..capacity` |
| 40 | 4 | crc32 | CRC32 of the payload |
| 44 | capacity | data | payload |
| ... | padding | — | zero or stale bytes |

A slot is valid when the magic/version/header size match, `capacity ==
slot_size`, `data_len <= capacity`, and `sequence > 0`. New rings initialize
slots with `sequence = 0` so readers ignore them.

## Claim byte

One byte per slot at `claims_offset + index`:

| Value | Meaning |
| --- | --- |
| `0` | free/stable; a writer may reserve it, a reader may claim it |
| `1` | claimed by a reader |
| `2` | reserved/writing by the writer |

All state transitions and the slot selection scan happen while holding an
exclusive `flock` on `<ring>.lock`. The writer releases the lock while it writes
the header and payload, but the `2` state keeps readers out.

## Writer

1. Open the file, verify the layout, check `writer_pid` (refuse if another live
   process owns the ring unless forced), reset all claim bytes, write its own
   pid.
2. To publish: under the lock, take the next slot in round-robin order with
   claim `0`, set it to `2`, and unlock.
3. Write the slot header (new sequence, timestamp, length, CRC), then the
   payload.
4. Under the lock, set the claim byte to `0`. The chunk is now readable.
5. If no slot is free, drop the chunk.

The writer does not call `msync` or `fsync`. Visibility is through the shared
mapping.

## Reader

1. Under the lock, scan all slots for claim `0` with a valid header, a sequence
   greater than this reader's cursor, and an age no greater than `max_age_ms`.
   A timestamp more than one second in the future is rejected.
2. Choose the fresh slot with the lowest sequence, set its claim byte to `1`,
   advance the cursor, and unlock.
3. Copy `data_len` bytes and verify the CRC.
4. Under the lock, set the claim byte back to `0`.

Each reader has its own cursor. A writer that laps a reader causes the reader to
skip the overwritten sequences; the ring does not block for slow readers. A
corrupt chunk is skipped and counted.

## Crash behavior

- Stale claims (`1` or `2`) left by a dead process are cleared by the next
  writer when it opens the ring. This reset also clears claims held by live
  readers, so stop readers before replacing a writer.
- Readers cannot clear a stale `2` themselves, because they cannot know whether
  a writer is mid-write.
- Recreating the ring (`create --overwrite`) discards all data and the lock file
  is reused.
