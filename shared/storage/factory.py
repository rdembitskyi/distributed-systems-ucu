from shared.storage.in_memory_message_store import MessageStore


def get_messages_storage():
    # for now just in memory
    return MessageStore()
