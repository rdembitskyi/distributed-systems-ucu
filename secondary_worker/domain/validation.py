from dataclasses import dataclass


@dataclass
class ValidationResult:
    is_valid: bool
    error: str = ""
    is_duplicated: bool = False
