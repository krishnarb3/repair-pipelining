package distributed.erasure.coding.pipeline

import java.util.concurrent.CountDownLatch

interface Node {
    fun fetchBlock(blockId: String, blockSize: Int, subpacketIndex: Int)
    fun setLatch(latch: CountDownLatch)
}