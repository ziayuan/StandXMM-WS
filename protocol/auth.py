import time
import json
import base64
import base58
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

class StandXAuth:
    """
    Handles authentication using StandX API Key (Ed25519) & Secret.
    """
    def __init__(self, api_key: str, api_secret: str, chain: str = "bsc"):
        self.api_key = api_key # JWT Token
        self.api_secret = api_secret # Private Key
        self.chain = chain
        
        # Load Private Key
        try:
            if api_secret.startswith("0x"):
                 secret_bytes = bytes.fromhex(api_secret[2:])
            else:
                 # Try Base58 First (StandX formatting)
                 try:
                    secret_bytes = base58.b58decode(api_secret)
                 except:
                    # Fallback to Base64
                    try:
                        secret_bytes = base64.b64decode(api_secret)
                    except:
                        secret_bytes = bytes.fromhex(api_secret)
            
            # Handle Lengths
            if len(secret_bytes) == 32:
                self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes)
            elif len(secret_bytes) == 64:
                 self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes[:32])
            else:
                 print(f"[Auth] Warning: Secret is {len(secret_bytes)} bytes. Using first 32.")
                 self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes[:32])

            # DERIVE PUBLIC KEY (Session ID)
            self._public_key = self._private_key.public_key()
            pub_bytes = self._public_key.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw
            )
            # StandX Session ID is usually the Base58 encoded Public Key
            self.public_id = base58.b58encode(pub_bytes).decode('utf-8')
            print(f"[Auth] Derived Session ID: {self.public_id}")

        except Exception as e:
            print(f"[Auth] Key Load Error: {e}")
            raise ValueError("Invalid API Secret. Check .env")

    def get_jwt(self, force_refresh: bool = False) -> str:
        return self.api_key # Return Token

    def get_public_id(self) -> str:
        return self.public_id

    def sign_request(self, payload: str, request_id: str, timestamp: int) -> dict:
        """
        Sign an API Payload with the Private Key (Ed25519).
        """
        version = "v1"
        message = f"{version},{request_id},{timestamp},{payload}"
        message_bytes = message.encode('utf-8')
        
        signature = self._private_key.sign(message_bytes)
        signature_b64 = base64.b64encode(signature).decode('utf-8')
        
        return {
            "x-request-sign-version": version,
            "x-request-id": request_id,
            "x-request-timestamp": str(timestamp),
            "x-request-signature": signature_b64,
            # Removed Authorization header as we are already logged in to WS
        }
