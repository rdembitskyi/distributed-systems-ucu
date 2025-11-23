from dataclasses import dataclass


@dataclass
class ValidationResult:
    is_valid: bool
    error: str = ""
    is_duplicated: bool = False

    corrupted_signature: bool = False
    parent_is_missing: bool = False
