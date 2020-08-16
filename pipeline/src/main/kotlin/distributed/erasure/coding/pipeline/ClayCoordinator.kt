package distributed.erasure.coding.pipeline

import com.backblaze.erasure.ReedSolomon
import distributed.erasure.coding.clay.ClayCode
import distributed.erasure.coding.clay.ClayCode.allocateOutputBuffer
import distributed.erasure.coding.clay.ClayCode.getChunks
import distributed.erasure.coding.clay.ClayCodeErasureDecodingStep
import distributed.erasure.coding.clay.ECBlock
import distributed.erasure.coding.clay.ECChunk
import java.io.DataInputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.ByteBuffer

class ClayCoordinator {
    val NUM_DATA_UNITS = 4
    val NUM_PARITY_UNITS = 2
    val SUBPACKET_SIZE = 8
    val BLOCK_SIZE = 8704

    var inputs = Array<ECBlock>((NUM_DATA_UNITS + NUM_PARITY_UNITS) * SUBPACKET_SIZE) { ECBlock(false, false) }

    fun fetchBlockUsingPipelining(finalNodeId: Int, erasedIndex: Int, blockId: String) {
        val erasedIndexes = intArrayOf(erasedIndex)
        val isDirect = true
        val util = ClayCodeErasureDecodingStep.ClayCodeUtil(
            erasedIndexes, NUM_DATA_UNITS, NUM_PARITY_UNITS
        )
        val originalInputs = ClayCode.getInputs()
        inputs = ClayCode.getTestInputs(originalInputs)

        val outputsArray = Array(SUBPACKET_SIZE) { Array(erasedIndexes.size) { ByteBuffer.wrap(ByteArray(BLOCK_SIZE)) } }
        getHelperPlanesAndDecode(util, blockId, outputsArray, erasedIndex, BLOCK_SIZE, isDirect)

        println("Completed")
    }

    private fun getInputsFromNodes(
        util: ClayCodeErasureDecodingStep.ClayCodeUtil,
        blockId: String,
        subpacketIndex: Int,
        erasedIndex: Int,
        bufSize: Int
    ): Array<ByteArray> {
        val helperIndexes = util.getHelperPlanesIndexes(erasedIndex)
        val helperPlanes = Array(helperIndexes.size) { ByteArray(bufSize) }

        for (i in helperIndexes.indices) {
//            helperPlanes[i] = getInput(blockId, subpacketIndex, helperIndexes[i])
        }

        return helperPlanes
    }

    private fun getInput(blockId: String, subpacketIndex: Int, index: Int): Array<ByteBuffer> {
        val newIn = Array(SUBPACKET_SIZE) { Array(NUM_DATA_UNITS + NUM_PARITY_UNITS) { ByteBuffer.allocate(BLOCK_SIZE) } }
        for (i in 0 until SUBPACKET_SIZE) {
            for (j in 0 until NUM_DATA_UNITS + NUM_PARITY_UNITS) {
                newIn[i][j] = inputs[i * (NUM_DATA_UNITS + NUM_PARITY_UNITS) + j].chunk.buffer
            }
        }
        val inputs = newIn[index]
        return inputs
    }

    private fun getNodesForBlock(blockId: String): List<Pair<Int, String>> {
        TODO("Get nodes for blockId")
    }

    @Throws(IOException::class)
    private fun getHelperPlanesAndDecode(
        util: ClayCodeErasureDecodingStep.ClayCodeUtil,
        blockId: String,
        outputs: Array<Array<ByteBuffer>>,
        erasedIndex: Int, bufSize: Int, isDirect: Boolean
    ) {
        val helperIndexes = util.getHelperPlanesIndexes(erasedIndex)
        val helperCoupledPlanes = Array(helperIndexes.size) {
            Array(NUM_DATA_UNITS + NUM_PARITY_UNITS) { ByteBuffer.allocate(bufSize) }
        }

        val pairWiseDecoder = ReedSolomon.create(2, 2)
        val rsRawDecoder = ReedSolomon.create(NUM_DATA_UNITS, NUM_PARITY_UNITS)
        val clayCodeErasureDecodingStep = ClayCodeErasureDecodingStep(
            intArrayOf(erasedIndex), pairWiseDecoder, rsRawDecoder
        )
        getHelperPlanes(util, blockId, helperCoupledPlanes, erasedIndex)
        clayCodeErasureDecodingStep.doDecodeSingle(
            helperCoupledPlanes,
            helperIndexes,
            outputs,
            erasedIndex,
            bufSize,
            isDirect
        )
    }

    private fun getHelperPlanes(
        util: ClayCodeErasureDecodingStep.ClayCodeUtil,
        blockId: String,
        helperCoupledPlanes: Array<Array<ByteBuffer>>,
        erasedIndex: Int
    ) {
        val helperIndexes = util.getHelperPlanesIndexes(erasedIndex)

        for (i in helperIndexes.indices) {
            helperCoupledPlanes[i] = getInput(blockId, i, helperIndexes[i])
        }
    }
}

fun main() {
    val coordinator = ClayCoordinator()
    val finalNodeId = 3
    coordinator.fetchBlockUsingPipelining(3, 1, "dummy")
}