package distributed.erasure.coding

import kotlin.experimental.xor

fun main() {
    val byte1 = "1".toInt().toByte()
    val byte2 = "7".toInt().toByte()
    val byte3 = "8".toInt().toByte()
    val parity = byte1.xor(byte2).xor(byte3)

    println("Parity: $parity")
    val res = byte1.xor(byte3).xor(parity)
    println("Result: ${res.toInt()}")
}