# Contributing

Issues and focused pull requests are welcome.

## Before opening a pull request

```sh
python -m pip install -e '.[dev]'
python -m pytest
python examples/demo.py
```

Tests use temporary files only. Do not point a test at a shared ring, and do not
add hardware or network dependencies to the default test path.

## Protocol changes

The on-disk layout is documented in `docs/FORMAT.md`. Any change to a struct,
offset, claim state, or cursor rule needs:

- a format version bump or an explicit compatibility statement;
- a test for the allowed and rejected cases;
- an update to the format document.

## Scope

This package is the transport: a single writer, fixed slots, claim-based readers,
freshness, CRC, and discovery. Task queues, acknowledgement/retry, a socket
fallback, and hardware adapters belong in separate projects.

## Reporting bugs

Include the OS, Python version, ring geometry (`mmap-entropy stat`), whether the
writer was running, and the expected versus actual sequence or payload. Remove
paths and data that you do not want to publish.
