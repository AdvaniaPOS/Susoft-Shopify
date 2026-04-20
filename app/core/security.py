"""
Security Module
================
Handles encryption/decryption of sensitive data (API keys, tokens)
and webhook signature verification.

Uses Fernet symmetric encryption for storing API credentials securely
in the database. The encryption key must be kept safe and consistent
across deployments.
"""

import hashlib
import hmac
import base64
import secrets
from typing import Optional
from datetime import datetime, timedelta

from cryptography.fernet import Fernet, InvalidToken
from passlib.context import CryptContext
from jose import jwt, JWTError

from app.core.config import settings


# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class EncryptionError(Exception):
    """Raised when encryption/decryption fails."""
    pass


class SignatureVerificationError(Exception):
    """Raised when signature verification fails."""
    pass


class CredentialEncryption:
    """
    Handles encryption and decryption of sensitive credentials.
    
    Uses Fernet (symmetric encryption) with the key from settings.
    All API keys and tokens should be encrypted before storage
    and decrypted only when needed for API calls.
    
    Example:
        cipher = CredentialEncryption()
        encrypted = cipher.encrypt("my-api-key")
        decrypted = cipher.decrypt(encrypted)
    """
    
    def __init__(self, key: Optional[str] = None):
        """
        Initialize with encryption key.
        
        Args:
            key: Optional Fernet key. Defaults to settings.encryption_key.
        """
        key_value = key or settings.encryption_key.get_secret_value()
        
        # Validate the key is a valid Fernet key
        try:
            self._fernet = Fernet(key_value.encode() if isinstance(key_value, str) else key_value)
        except Exception as e:
            raise EncryptionError(
                f"Invalid encryption key. Generate one with: "
                f"python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            ) from e
    
    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a plaintext string.
        
        Args:
            plaintext: The string to encrypt.
            
        Returns:
            Base64-encoded encrypted string.
            
        Raises:
            EncryptionError: If encryption fails.
        """
        if plaintext is None:
            raise EncryptionError("Cannot encrypt None value")
        
        try:
            encrypted_bytes = self._fernet.encrypt(plaintext.encode("utf-8"))
            return encrypted_bytes.decode("utf-8")
        except Exception as e:
            raise EncryptionError(f"Encryption failed: {e}") from e
    
    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt an encrypted string.
        
        Args:
            ciphertext: The encrypted string to decrypt.
            
        Returns:
            Decrypted plaintext string.
            
        Raises:
            EncryptionError: If decryption fails (invalid key or corrupted data).
        """
        if not ciphertext:
            raise EncryptionError("Cannot decrypt empty string")
        
        try:
            decrypted_bytes = self._fernet.decrypt(ciphertext.encode("utf-8"))
            return decrypted_bytes.decode("utf-8")
        except InvalidToken:
            raise EncryptionError(
                "Decryption failed: Invalid token. "
                "This may indicate corrupted data or wrong encryption key."
            )
        except Exception as e:
            raise EncryptionError(f"Decryption failed: {e}") from e
    
    def rotate_key(self, old_ciphertext: str, new_cipher: "CredentialEncryption") -> str:
        """
        Re-encrypt data with a new key (for key rotation).
        
        Args:
            old_ciphertext: Data encrypted with the current key.
            new_cipher: CredentialEncryption instance with the new key.
            
        Returns:
            Data encrypted with the new key.
        """
        plaintext = self.decrypt(old_ciphertext)
        return new_cipher.encrypt(plaintext)


# Global encryption instance
_cipher: Optional[CredentialEncryption] = None


def get_cipher() -> CredentialEncryption:
    """Get or create the global encryption instance."""
    global _cipher
    if _cipher is None:
        _cipher = CredentialEncryption()
    return _cipher


def encrypt_credential(plaintext: str) -> str:
    """Convenience function to encrypt a credential."""
    return get_cipher().encrypt(plaintext)


def decrypt_credential(ciphertext: str) -> str:
    """Convenience function to decrypt a credential."""
    return get_cipher().decrypt(ciphertext)


class WebhookSignatureVerifier:
    """
    Verifies webhook signatures from Shopify and Susoft.
    
    Shopify uses HMAC-SHA256 with the webhook secret.
    Susoft may use different verification methods.
    """
    
    @staticmethod
    def verify_shopify_signature(
        payload: bytes,
        signature: str,
        secret: str
    ) -> bool:
        """
        Verify Shopify webhook HMAC signature.
        
        Shopify sends the signature in the X-Shopify-Hmac-SHA256 header.
        
        Args:
            payload: Raw request body bytes.
            signature: Base64-encoded signature from header.
            secret: Shopify webhook secret.
            
        Returns:
            True if signature is valid.
            
        Raises:
            SignatureVerificationError: If verification fails.
        """
        try:
            computed_hmac = hmac.new(
                secret.encode("utf-8"),
                payload,
                hashlib.sha256
            ).digest()
            computed_signature = base64.b64encode(computed_hmac).decode("utf-8")
            
            # Use compare_digest to prevent timing attacks
            return hmac.compare_digest(computed_signature, signature)
        except Exception as e:
            raise SignatureVerificationError(f"Shopify signature verification failed: {e}")
    
    @staticmethod
    def verify_susoft_token(
        provided_token: str,
        expected_token: str
    ) -> bool:
        """
        Verify Susoft webhook token.
        
        Susoft typically sends a token in the webhook configuration
        that must match what we expect.
        
        Args:
            provided_token: Token from the webhook request.
            expected_token: Expected token from our configuration.
            
        Returns:
            True if tokens match.
        """
        if not provided_token or not expected_token:
            return False
        
        # Use compare_digest to prevent timing attacks
        return hmac.compare_digest(provided_token, expected_token)


# ===================
# JWT Token Functions
# ===================

def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Create a JWT access token for admin authentication.
    
    Args:
        data: Payload data to encode.
        expires_delta: Token expiration time.
        
    Returns:
        Encoded JWT token.
    """
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.admin_session_timeout_minutes)
    
    to_encode.update({"exp": expire})
    
    encoded_jwt = jwt.encode(
        to_encode,
        settings.secret_key.get_secret_value(),
        algorithm="HS256"
    )
    return encoded_jwt


def verify_access_token(token: str) -> Optional[dict]:
    """
    Verify and decode a JWT access token.
    
    Args:
        token: JWT token to verify.
        
    Returns:
        Decoded payload if valid, None otherwise.
    """
    try:
        payload = jwt.decode(
            token,
            settings.secret_key.get_secret_value(),
            algorithms=["HS256"]
        )
        return payload
    except JWTError:
        return None


# ===================
# Password Functions
# ===================

def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


# ===================
# Utility Functions
# ===================

def generate_webhook_secret() -> str:
    """Generate a secure random webhook secret."""
    return secrets.token_urlsafe(32)


def generate_api_key() -> str:
    """Generate a secure random API key."""
    return secrets.token_urlsafe(48)


# ===================
# Convenience Functions
# ===================

# Singleton instance for credential encryption
_credential_encryptor = CredentialEncryption()


def encrypt_credential(credential: str) -> str:
    """
    Encrypt a credential (API key, secret, etc.).
    
    Convenience function wrapping CredentialEncryption.
    
    Args:
        credential: Plain text credential to encrypt.
        
    Returns:
        Encrypted credential string.
    """
    if not credential:
        # Handle empty string - encrypt empty value
        return _credential_encryptor.encrypt("")
    return _credential_encryptor.encrypt(credential)


def decrypt_credential(encrypted_credential: str) -> str:
    """
    Decrypt an encrypted credential.
    
    Convenience function wrapping CredentialEncryption.
    
    Args:
        encrypted_credential: Encrypted credential string.
        
    Returns:
        Decrypted plain text credential.
    """
    return _credential_encryptor.decrypt(encrypted_credential)


def verify_shopify_webhook(
    payload: bytes,
    signature: str,
    secret: str
) -> bool:
    """
    Verify a Shopify webhook signature.
    
    Convenience function wrapping WebhookSignatureVerifier.
    
    Args:
        payload: Raw request body bytes.
        signature: HMAC signature from X-Shopify-Hmac-SHA256 header.
        secret: Shopify webhook secret.
        
    Returns:
        True if signature is valid, False otherwise.
    """
    try:
        return WebhookSignatureVerifier.verify_shopify_signature(
            payload=payload,
            signature=signature,
            secret=secret
        )
    except SignatureVerificationError:
        return False


def get_password_hash(password: str) -> str:
    """
    Hash a password.
    
    Alias for hash_password for compatibility.
    
    Args:
        password: Plain text password.
        
    Returns:
        Hashed password string.
    """
    return hash_password(password)
