# AES 128 CBC using the Fernet implementation
# https://github.com/fernet/spec/blob/master/Spec.md

from cryptography.fernet import Fernet
from base64 import b64encode, b64decode, urlsafe_b64encode
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

class FernetCryptoLib:
    def encrypt(self):
        print("a")

def makekey(key):
    keybytes = key.encode('UTF-8')
    salt = os.urandom(16) # produce 16 pseudorandom bytes

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=390000,
    )

    key = urlsafe_b64encode(kdf.derive(keybytes))

    return key

key = makekey("helloworld")
f = Fernet(key)

token = f.encrypt(b"secret text here")
print(f.decrypt(token).decode('UTF-8'))