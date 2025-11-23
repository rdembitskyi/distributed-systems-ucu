from dataclasses import dataclass
from enum import Enum

from shared.domain.response import SyncStatus


@dataclass
class SyncResult:
    status: SyncStatus
    failed_messages: list | None = None
