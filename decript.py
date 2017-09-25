#!/usr/bin/env python
# -*- coding:utf-8 -*-

from binascii import a2b_hex
from hashlib import md5

from Crypto.Cipher import AES


def decrypt(key, cipher_text):
    decrypt_key = md5(key).digest()
    crypto_maker = AES.new(decrypt_key, AES.MODE_CBC, b'0000000000000000')
    plain_text = crypto_maker.decrypt(a2b_hex(cipher_text))
    return plain_text.rstrip('\0')


if __name__ == '__main__':
    print decrypt('oV9CowOpGj0e6hejckfqwoE9x3D4', '810b747cef94fb364c4e76fbebd1e8cf')
