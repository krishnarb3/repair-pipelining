package distributed.erasure.coding.pipeline

import redis.clients.jedis.Jedis
import kotlin.math.pow

class PipelineUtil(jedis: Jedis) {
    companion object {
        const val JEDIS_POOL_MAX_SIZE = 10

        const val BLOCK_SIZE = 34816
        const val WORD_LENGTH = BLOCK_SIZE / 1024

        const val CLAY_BLOCK_SIZE = "CLAY_BLOCK_SIZE"
        const val FILE_READ_BUFFER_SIZE = 1024 * 256

        const val NUM_DATA_UNITS = "NUM_DATA_UNITS"
        const val NUM_PARITY_UNITS = "NUM_PARITY_UNITS"

        val COORDINATOR_CHANNEL_NAME = "coordinator"
        val HELPER_CHANNEL_PREFIX = "helper"
    }

    val numDataUnits = jedis[NUM_DATA_UNITS].toInt()
    val numParityUnits = jedis[NUM_PARITY_UNITS].toInt()
    val numTotalUnits = numDataUnits + numParityUnits

    val subpacketSize = numParityUnits.toDouble().pow(numTotalUnits/numParityUnits).toInt()
    val clayBlockSize = jedis[CLAY_BLOCK_SIZE].toInt()
}