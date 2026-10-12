"""Security utilities for credential encryption and safe handling."""

import base64
import hashlib
from cryptography.fernet import Fernet
from volmigrate.core.config import get_settings


def _get_fernet() -> Fernet:
    settings = get_settings()
    key_material = hashlib.sha256(settings.SECRET_KEY.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key_material)
    return Fernet(fernet_key)


def encrypt_secret(plain_text: str) -> str:
    """Encrypt sensitive string (e.g., token, kubeconfig, password)."""
    if not plain_text:
        return ""
    fernet = _get_fernet()
    return fernet.encrypt(plain_text.encode("utf-8")).decode("utf-8")


def decrypt_secret(cipher_text: str) -> str:
    """Decrypt sensitive string."""
    if not cipher_text:
        return ""
    fernet = _get_fernet()
    return fernet.decrypt(cipher_text.encode("utf-8")).decode("utf-8")


def mask_secret(value: str) -> str:
    """Return a masked representation for logging or display."""
    if not value or len(value) <= 6:
        return "******"
    return f"{value[:3]}...{value[-3:]}"
