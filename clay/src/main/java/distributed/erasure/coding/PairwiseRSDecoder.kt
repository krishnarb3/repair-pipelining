package distributed.erasure.coding

import com.backblaze.erasure.ReedSolomon

class PairwiseRSDecoder(val numDataUnits: Int, val numParityUnits: Int) {
    fun decode(inputs: Array<ByteArray>, erasedIndexes: Array<Int>, blockSize: Int) {
        val rs = ReedSolomon.create(numDataUnits, numParityUnits)
        val shardsPresent = (inputs.indices).map { !erasedIndexes.contains(it) }.toBooleanArray()
        rs.decodeMissing(inputs, shardsPresent, 0, blockSize)
    }
}