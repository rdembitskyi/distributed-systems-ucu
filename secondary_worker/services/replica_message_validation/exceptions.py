class InvalidSignatureError(Exception):
    """Raised when a message has an invalid signature."""


class InvalidParentIdError(Exception):
    """Raised when a message has an invalid parent id."""


class InvalidSequenceNumberError(Exception):
    """Raised when a message has an invalid sequence number."""


class DuplicateMessageError(Exception):
    """Raised when a message with the same sequence or id already exists in the store."""
