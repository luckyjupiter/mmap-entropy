# Security policy

This package is a local transport, not a security boundary.

- `flock` is advisory. Any process that can write the ring file can corrupt it.
- Readers open the ring read-write because the claim map is in the same mapping.
  Use file ownership and group permissions; do not place a ring on a shared or
  untrusted filesystem.
- There is no encryption, authentication, or network transport. Keep rings on a
  local filesystem.
- A ring contains whatever the producer writes. Do not treat it as a trust
  boundary between users.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting for this repository. Do not open a
public issue for a memory-safety, desynchronization, or data-exposure defect
before a fix is available.

Include the OS, Python version, ring geometry, writer/reader lifecycle, and a
minimal reproduction. Do not include private payloads.

## Supported versions

Only the latest release receives fixes.
