package app.load.providers

import javax.crypto.Cipher

class CipherInstanceProvider {
    fun cipherInstance(): Cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")
}
