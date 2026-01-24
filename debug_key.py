import os
import base64
import base58
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

def analyze_key():
    api_secret = os.getenv("STANDX_API_SECRET")
    if not api_secret:
        print("❌ Error: STANDX_API_SECRET not found in .env")
        return

    print(f"🔍 Analyzing Secret (Length: {len(api_secret)} chars)...")
    
    # 1. Decode
    try:
        if api_secret.startswith("0x"):
            secret_bytes = bytes.fromhex(api_secret[2:])
            print("✅ Format: HEX")
        else:
            # Try Base58 first (StandX uses Base58 for PubKeys, maybe Secrets too?)
            try:
                secret_bytes = base58.b58decode(api_secret)
                print("✅ Format: Base58")
            except:
                try:
                    secret_bytes = base64.b64decode(api_secret)
                    print("✅ Format: Base64")
                except:
                    secret_bytes = bytes.fromhex(api_secret)
                    print("✅ Format: Raw HEX")
    except Exception as e:
        print(f"❌ Decode Failed: {e}")
        return

    # 2. Analyze Bytes
    length = len(secret_bytes)
    print(f"📊 Decoded Length: {length} bytes")
    print(f"🔑 First Byte: 0x{secret_bytes[0]:02x}")
    print(f"🔑 Last Byte:  0x{secret_bytes[-1]:02x}")

    if length != 32 and length != 33:
        print("⚠️ Warning: Ed25519 Seeds are typically 32 bytes.")

    # 3. Derive Candidates
    candidates = []
    
    # Case A: First 32 bytes
    if length >= 32:
        try:
            seed_a = secret_bytes[:32]
            priv_a = ed25519.Ed25519PrivateKey.from_private_bytes(seed_a)
            pub_a = priv_a.public_key().public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw
            )
            pub_b58_a = base58.b58encode(pub_a).decode('utf-8')
            candidates.append(("[0:32] (Drop End)", pub_b58_a))
        except Exception as e:
            print(f"Option A Failed: {e}")

    # Case B: Last 32 bytes (if length > 32)
    if length >= 33:
        try:
            seed_b = secret_bytes[1:33]
            priv_b = ed25519.Ed25519PrivateKey.from_private_bytes(seed_b)
            pub_b = priv_b.public_key().public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw
            )
            pub_b58_b = base58.b58encode(pub_b).decode('utf-8')
            candidates.append(("[1:33] (Drop Start)", pub_b58_b))
        except Exception as e:
            print(f"Option B Failed: {e}")

    print("\n🧐 Candidate Public Keys (Session IDs):")
    for label, pub in candidates:
        print(f"   👉 {label}: {pub}")

    print("\n💡 ACTION REQUIRED:")
    print("Please check your StandX Dashboard or saved keys.")
    print("Which of the above Public Keys matches your API Key Address?")

if __name__ == "__main__":
    analyze_key()
