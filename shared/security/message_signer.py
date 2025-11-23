import base64
import json
import logging
import os

from cryptography.fernet import Fernet

from shared.domain.messages import Message


logger = logging.getLogger(__name__)


class FernetMessageSigner:
    def __init__(self):
        # Get key from environment or generate one
        key = os.getenv("FERNET_KEY")
        if not key:
            raise ValueError("FERNET_KEY environment variable is not set")
        else:
            key = key.encode()

        self.fernet = Fernet(key)

    def sign_message(self, message: Message) -> Message:
        """Sign message content, sequence_number, id, parent_id"""
        # Create payload to sign
        logger.info(f"Signing message: {message.content}")
        payload = {
            "message_id": message.message_id,
            "content": message.content,
            "sequence_number": message.sequence_number,
            "parent_id": message.parent_id,
        }

        # Convert to JSON and encrypt
        payload_json = json.dumps(payload, sort_keys=True)
        signature = self.fernet.encrypt(payload_json.encode())
        message.signature = signature.decode()

        return message

    def verify_signature(self, message: Message, signature: str) -> bool:
        """Verify message signature on replica"""
        try:
            payload = {
                "message_id": message.message_id,
                "content": message.content,
                "sequence_number": message.sequence_number,
                "parent_id": message.parent_id,
            }

            expected_json = json.dumps(payload, sort_keys=True)

            # Decode and decrypt signature
            decrypted_payload = self.fernet.decrypt(signature.encode())

            # Compare payloads
            is_valid = decrypted_payload.decode() == expected_json
            return is_valid

        except Exception as e:
            logger.error(f"Error verifying signature: {e}")
            return False
