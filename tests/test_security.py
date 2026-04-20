"""
Unit tests for security module.
"""

import pytest
from app.core.security import (
    encrypt_credential,
    decrypt_credential,
    verify_shopify_webhook,
    create_access_token,
    verify_password,
    get_password_hash
)


class TestCredentialEncryption:
    """Tests for credential encryption/decryption."""
    
    def test_encrypt_decrypt_roundtrip(self):
        """Test that encryption and decryption work correctly."""
        original = "my-secret-api-key"
        encrypted = encrypt_credential(original)
        decrypted = decrypt_credential(encrypted)
        
        assert decrypted == original
        assert encrypted != original
    
    def test_encrypted_value_is_different(self):
        """Test that encrypted value differs from original."""
        credential = "test-credential"
        encrypted = encrypt_credential(credential)
        
        assert encrypted != credential
        assert len(encrypted) > len(credential)
    
    def test_empty_string(self):
        """Test handling of empty string."""
        encrypted = encrypt_credential("")
        decrypted = decrypt_credential(encrypted)
        
        assert decrypted == ""


class TestPasswordHashing:
    """Tests for password hashing."""
    
    def test_hash_and_verify(self):
        """Test password hashing and verification."""
        password = "secure-password-123"
        hashed = get_password_hash(password)
        
        assert verify_password(password, hashed)
        assert not verify_password("wrong-password", hashed)
    
    def test_hash_is_different_each_time(self):
        """Test that hashing the same password gives different results."""
        password = "test-password"
        hash1 = get_password_hash(password)
        hash2 = get_password_hash(password)
        
        # bcrypt uses random salt, so hashes should differ
        assert hash1 != hash2
        
        # But both should verify correctly
        assert verify_password(password, hash1)
        assert verify_password(password, hash2)


class TestJWTTokens:
    """Tests for JWT token creation."""
    
    def test_create_access_token(self):
        """Test JWT access token creation."""
        data = {"sub": "user123", "tenant_id": "tenant456"}
        token = create_access_token(data)
        
        assert token is not None
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_token_contains_claims(self):
        """Test that token contains expected claims."""
        import jwt
        from app.core.config import settings
        
        data = {"sub": "user123"}
        token = create_access_token(data)
        
        decoded = jwt.decode(
            token,
            settings.secret_key.get_secret_value(),
            algorithms=["HS256"]
        )
        
        assert decoded["sub"] == "user123"
        assert "exp" in decoded


class TestWebhookVerification:
    """Tests for Shopify webhook verification."""
    
    def test_verify_valid_signature(self):
        """Test verification of valid Shopify webhook signature."""
        import hmac
        import hashlib
        import base64
        
        # Create test payload and secret
        payload = b'{"test": "data"}'
        secret = "test-secret"
        
        # Create valid signature
        computed_hmac = hmac.new(
            secret.encode("utf-8"),
            payload,
            hashlib.sha256
        ).digest()
        valid_signature = base64.b64encode(computed_hmac).decode("utf-8")
        
        # Verify
        result = verify_shopify_webhook(payload, valid_signature, secret)
        assert result is True
    
    def test_reject_invalid_signature(self):
        """Test rejection of invalid webhook signature."""
        payload = b'{"test": "data"}'
        invalid_signature = "invalid-signature-here"
        secret = "test-secret"
        
        result = verify_shopify_webhook(payload, invalid_signature, secret)
        assert result is False
