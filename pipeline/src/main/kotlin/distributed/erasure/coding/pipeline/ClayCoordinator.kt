package distributed.erasure.coding.pipeline

import distributed.erasure.coding.clay.ClayCode
import distributed.erasure.coding.clay.ClayCodeErasureDecodingStep
import distributed.erasure.coding.clay.ClayCodeHelper
import distributed.erasure.coding.clay.ECBlock
import distributed.erasure.coding.pipeline.Util.CLAY_BLOCK_SIZE
import distributed.erasure.coding.pipeline.Util.NUM_DATA_UNITS
import distributed.erasure.coding.pipeline.Util.NUM_PARITY_UNITS
import distributed.erasure.coding.pipeline.Util.NUM_TOTAL_UNITS
import distributed.erasure.coding.pipeline.Util.SUBPACKET_SIZE
import java.nio.ByteBuffer

class ClayCoordinator(
    nodeHostMap: MutableMap<Int, Pair<String, Int>>,
    blockNodeMap: MutableMap<String, Int>,
    val blockIndexMap: MutableMap<String, Int>
) : Coordinator(nodeHostMap, blockNodeMap) {

    var inputs = Array((NUM_DATA_UNITS + NUM_PARITY_UNITS) * SUBPACKET_SIZE) {
        ECBlock(false, false)
    }

    override fun fetchBlockUsingPipelining(finalNodeId: Int, blockId: String) {
        val erasedIndex = blockIndexMap[blockId] ?: throw Exception("Index not found for block")
        fetchBlockUsingPipelining(finalNodeId, erasedIndex, blockId)
    }

    fun fetchBlockUsingPipelining(finalNodeId: Int, erasedIndex: Int, blockId: String) {
        val erasedIndexes = intArrayOf(erasedIndex)
        val isDirect = true
        val util = ClayCodeErasureDecodingStep.ClayCodeUtil(
            erasedIndexes, NUM_DATA_UNITS, NUM_PARITY_UNITS
        )
        val parityIndexes = (NUM_DATA_UNITS until NUM_TOTAL_UNITS).toList().toIntArray()
        var clayCode = ClayCode(NUM_DATA_UNITS, NUM_PARITY_UNITS, CLAY_BLOCK_SIZE, parityIndexes)
        val originalInputs = clayCode.getInputs()
        val originalOutputs = clayCode.getOutputs()
        val encodedResult = clayCode.encode(originalInputs, originalOutputs)

        inputs = clayCode.getTestInputs(encodedResult[0], encodedResult[1], erasedIndexes)

        val outputsArray =
            Array(SUBPACKET_SIZE) { Array(erasedIndexes.size) { ByteBuffer.wrap(ByteArray(CLAY_BLOCK_SIZE)) } }

        val clayCodeHelper = ClayCodeHelper(
            NUM_DATA_UNITS, NUM_PARITY_UNITS, SUBPACKET_SIZE, inputs
        )
        clayCodeHelper.getHelperPlanesAndDecode(util, blockId, outputsArray, erasedIndex, CLAY_BLOCK_SIZE, isDirect)

        println("Completed")
    }
}

fun main() {
    val nodeHostMap = mutableMapOf(
        0 to Pair("127.0.0.1", 1111),
        1 to Pair("127.0.0.1", 2222),
        2 to Pair("127.0.0.1", 3333),
        3 to Pair("127.0.0.1", 4444),
        4 to Pair("127.0.0.1", 5555),
        5 to Pair("127.0.0.1", 6666)
    )
    val blockNodeMap = LinkedHashMap<String, Int>()
    val blockIndexMap = mutableMapOf<String, Int>()
    (0..NUM_TOTAL_UNITS * SUBPACKET_SIZE).forEach {
        blockNodeMap["$it-LP.jpg"] = it % NUM_TOTAL_UNITS
        blockNodeMap["$it-LP.jpg"] = it
    }
    val coordinator = ClayCoordinator(nodeHostMap, blockNodeMap, blockIndexMap)
    coordinator.fetchBlockUsingPipelining(3, 1, "dummy")
}