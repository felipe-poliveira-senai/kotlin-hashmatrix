package org.example.io.senai.istic.ext

fun ByteArray.toHexString(): String {
    val hexString = StringBuilder(this.size * 2)
    for (byte in this) {
        hexString.append(String.format("%02x", byte))
    }
    return hexString.toString()
}