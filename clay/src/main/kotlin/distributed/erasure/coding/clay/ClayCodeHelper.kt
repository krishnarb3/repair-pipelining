package distributed.erasure.coding.clay

import com.backblaze.erasure.ReedSolomon
import java.nio.ByteBuffer

class ClayCodeHelper(
    val NUM_DATA_UNITS: Int,
    val NUM_PARITY_UNITS: Int,
    val SUBPACKET_SIZE: Int,
    val inputs: Array<ECBlock>
) {
    val NUM_TOTAL_UNITS = NUM_DATA_UNITS + NUM_PARITY_UNITS
    val pairWiseDecoder = ReedSolomon.create(2, 2)
    val rsRawDecoder = ReedSolomon.create(NUM_DATA_UNITS, NUM_PARITY_UNITS)

    fun getHelperPlanesAndDecode(
        util: ClayCodeErasureDecodingStep.ClayCodeUtil,
        blockId: String,
        outputs: Array<Array<ByteBuffer>>,
        erasedIndex: Int, bufSize: Int, isDirect: Boolean
    ) {
        val helperIndexes = util.getHelperPlanesIndexes(erasedIndex)
        val helperCoupledPlanes = Array(helperIndexes.size) {
            Array(NUM_DATA_UNITS + NUM_PARITY_UNITS) { ByteBuffer.allocate(bufSize) }
        }

        val clayCodeErasureDecodingStep = ClayCodeErasureDecodingStep(
            intArrayOf(erasedIndex), pairWiseDecoder, rsRawDecoder
        )

        for (i in helperIndexes.indices) {
            val z = helperIndexes[i]
            val z_vec = util.getZVector(z)

            val coupleCoordinates = (0 until util.q * util.t).map {  j ->
                val coordinates = util.getNodeCoordinates(j)
                util.getNodeIndex(z_vec[coordinates[1]], coordinates[1])
            }.toSet()

//            val indices = (setOf(z) + coupleCoordinates).toList()
            val indices = (0 until NUM_TOTAL_UNITS).toList()

            getHelperPlanes(blockId, helperCoupledPlanes, helperIndexes, indices)

            clayCodeErasureDecodingStep.doDecodeSingle(
                helperCoupledPlanes,
                helperIndexes,
                i,
                outputs,
                erasedIndex,
                bufSize,
                isDirect
            )

            println("Completed decode single: $i")
        }

        println("Completed decode single for all")
    }

    private fun getHelperPlanes(
        blockId: String,
        helperCoupledPlanes: Array<Array<ByteBuffer?>>,
        helperIndexes: IntArray,
        indices: List<Int>
    ) {
        for (i in helperIndexes.indices) {
            for (j in indices.indices) {
                helperCoupledPlanes[i][j] = getInput(blockId, 0, helperIndexes[i], j)
            }
        }
    }

    private fun getInput(blockId: String, subpacketIndex: Int, xIndex: Int, yIndex: Int): ByteBuffer? {
        // TODO: Fix this method
        val newIn = Array(SUBPACKET_SIZE) { arrayOfNulls<ByteBuffer>(NUM_DATA_UNITS + NUM_TOTAL_UNITS) }
        for (i in 0 until SUBPACKET_SIZE) {
            for (j in 0 until NUM_DATA_UNITS + NUM_PARITY_UNITS) {
                newIn[i][j] = inputs[i * (NUM_DATA_UNITS + NUM_PARITY_UNITS) + j].chunk.buffer
            }
        }
        val input = newIn[xIndex][yIndex]
        return input
    }
}