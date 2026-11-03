"""Exceptions raised by mmap-entropy."""


class MmapEntropyError(Exception):
    """Base class for transport errors."""


class RingFormatError(MmapEntropyError):
    """The ring file is missing, truncated, or has an unsupported format."""


class WriterBusyError(MmapEntropyError):
    """Another live process appears to own the ring."""


class CorruptSlotError(MmapEntropyError):
    """A slot failed length or CRC validation."""
