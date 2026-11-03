"""Byte sources for the writer."""

from __future__ import annotations

import os
import random
from typing import Callable, Protocol


class Source(Protocol):
    """Anything with ``read(n) -> bytes`` can feed the writer."""

    name: str

    def read(self, n: int) -> bytes: ...


class SyntheticSource:
    """Deterministic bytes for tests and demos. Not a random number generator."""

    name = "synthetic"

    def __init__(self, seed: int = 0) -> None:
        self._random = random.Random(seed)

    def read(self, n: int) -> bytes:
        return self._random.randbytes(max(0, n))


class UrandomSource:
    """OS entropy via ``os.urandom``."""

    name = "urandom"

    def read(self, n: int) -> bytes:
        return os.urandom(max(0, n))


class CallableSource:
    """Adapter for an existing function that returns bytes."""

    name = "callable"

    def __init__(self, function: Callable[[int], bytes]) -> None:
        self._function = function

    def read(self, n: int) -> bytes:
        return self._function(n)


def make_source(name: str, *, seed: int | None = None) -> Source:
    if name == "synthetic":
        return SyntheticSource(seed or 0)
    if name == "urandom":
        return UrandomSource()
    raise ValueError(f"unknown source {name!r}; choose synthetic or urandom")
