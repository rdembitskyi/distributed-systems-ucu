import os


def get_auth_token():
    token = os.environ.get("AUTH_TOKEN")
    if not token:
        raise ValueError("AUTH_TOKEN environment variable is not set")
    return token


def validate_auth_token(token: str):
    return token == get_auth_token()
