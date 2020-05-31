package distributed.erasure.coding.pipeline

import java.util.concurrent.CountDownLatch

interface Node {
    fun repairAndPipeline(stripeIndex: Int, output: Array<ByteArray>, nodesInPipeline: List<Node>)
    fun fetchBlock(blockId: String)
    fun setLatch(latch: CountDownLatch)
}