from enum import Enum


class QuorumLevel(Enum):
    ONE = 1  # Master only
    QUORUM = 2  # Majority (configurable)
    ALL = 3  # All replicas
