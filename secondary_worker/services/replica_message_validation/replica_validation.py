import logging
from shared.security.message_signer import FernetMessageSigner
from shared.domain.messages import Message
from secondary_worker.domain.validation import ValidationResult
from shared.storage.factory import get_messages_storage
from secondary_worker.services.replica_message_validation.exceptions import (
    InvalidSignatureError,
    InvalidParentIdError,
    InvalidSequenceNumberError,
    DuplicateMessageError,
)

# Validation error constants
INVALID_SIGNATURE_ERROR_MESSAGE = (
    "Message signature verification failed - the message may have been tampered with"
)
MESSAGE_PARENT_NOT_FOUND_ERROR_MESSAGE = "Message parent not found in storage"
INVALID_SEQUENCE_NUMBER_ERROR_MESSAGE = "Message sequence number is invalid"


logger = logging.getLogger(__name__)


def validate_message(message: Message) -> ValidationResult:
    try:
        verify_message_signature(message=message)
        verify_message_sequence_order(message=message)
    except InvalidSignatureError:
        return ValidationResult(is_valid=False, error=INVALID_SIGNATURE_ERROR_MESSAGE)
    except InvalidParentIdError:
        logger.warning(f"Message {message} parent not found: {message.parent_id}")
        return ValidationResult(
            is_valid=False, error=MESSAGE_PARENT_NOT_FOUND_ERROR_MESSAGE
        )
    except InvalidSequenceNumberError:
        return ValidationResult(is_valid=True, is_duplicated=True)

    return ValidationResult(is_valid=True)


def verify_message_signature(message: Message) -> bool:
    """Verify message signature on replica"""
    signer = FernetMessageSigner()
    is_signature_valid = signer.verify_signature(
        message=message, signature=message.signature
    )
    if not is_signature_valid:
        raise InvalidSignatureError()
    return is_signature_valid


def verify_message_sequence_order(message: Message) -> bool:
    """Verify that the parent_id and sequence order are correct"""
    logger.info(f"Verifying message sequence order: {message}")
    if not message.parent_id and message.sequence_number == 1:
        return True

    storage = get_messages_storage()
    parent = storage.get_by_id(msg_id=message.parent_id)
    if not parent:
        logger.warning(f"Message {message} parent not found")
        raise InvalidParentIdError()

    if parent.sequence_number != message.sequence_number - 1:
        raise InvalidSequenceNumberError()

    return True


def verify_no_duplicate_message(message: Message) -> None:
    storage = get_messages_storage()

    if storage.get_by_id(message.message_id):
        raise DuplicateMessageError("Message with the same ID already exists")

    if storage.get_by_sequence(message.sequence_number):
        raise DuplicateMessageError(
            "Message with the same sequence number already exists"
        )
