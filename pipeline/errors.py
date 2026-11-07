"""Pipeline exception types."""


class PipelineError(Exception):
    pass


class TransientError(PipelineError):
    """Retryable failure (I/O, connectivity, lock conflict)."""


class FileRejected(PipelineError):
    """A whole source file is structurally unusable (bad encoding, bad header, bad JSON)."""
