package app.configuration

import javax.crypto.Cipher

class CipherInstanceProvider {
    fun cipherInstance(): Cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")
}
