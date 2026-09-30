"""Failure classes shared by source adapters and the queue worker."""


class NonRetryableJobError(ValueError):
    """This exact job partition cannot succeed without a change or review."""


class NonRetryableSourceError(NonRetryableJobError):
    """The current source partition cannot succeed without operator review."""


class RawResponseTooLarge(NonRetryableSourceError):
    """A complete raw response exceeded the configured byte cap."""


class CertifiedBatchTooLarge(NonRetryableJobError):
    """The candidate target batch exceeded its configured row or byte cap."""


class HistorySplitRequired(RuntimeError):
    """A bounded history query needs smaller durable work partitions."""
