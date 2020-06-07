package distributed.erasure.coding

import com.backblaze.erasure.ReedSolomon

class LRCErasureCode {
    val rs = ReedSolomon.create(LRCErasureUtil.R, 1)
    fun encodeParitySingle(shard: ByteArray, output: ByteArray, index: Int, blockSize: Int) {
        rs.encodeParitySingle(shard, output, index, 0, 0, blockSize)
    }
}