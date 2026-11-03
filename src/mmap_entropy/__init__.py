"""A fixed-layout mmap ring for passing byte chunks between local processes."""

from .errors import (
    CorruptSlotError,
    MmapEntropyError,
    RingFormatError,
    WriterBusyError,
)
from .layout import RingHeader, SlotHeader, create_ring, open_ring_header
from .reader import RingReader, Slot
from .sources import (
    CallableSource,
    Source,
    SyntheticSource,
    UrandomSource,
    make_source,
)
from .writer import RingWriter

__all__ = [
    "CallableSource",
    "CorruptSlotError",
    "MmapEntropyError",
    "RingFormatError",
    "RingHeader",
    "RingReader",
    "RingWriter",
    "Slot",
    "SlotHeader",
    "Source",
    "SyntheticSource",
    "UrandomSource",
    "WriterBusyError",
    "create_ring",
    "make_source",
    "open_ring_header",
]

__version__ = "0.1.0"
