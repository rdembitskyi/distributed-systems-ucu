from enum import Enum


class StatusCodes(Enum):
    OK = 200
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    DUPLICATE_RECEIVED = 409
    RATE_LIMITED = 429
    INTERNAL_SERVER_ERROR = 500
