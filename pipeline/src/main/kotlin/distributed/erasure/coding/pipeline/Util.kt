package distributed.erasure.coding.pipeline

object Util {
    const val BLOCK_SIZE = 34816
    const val WORD_LENGTH = BLOCK_SIZE / 1024
    const val NUM_DATA_UNITS = 4
    const val NUM_PARITY_UNITS = 2
    const val NUM_TOTAL_UNITS = NUM_DATA_UNITS + NUM_PARITY_UNITS
    const val SUBPACKET_SIZE = 8
    const val CLAY_BLOCK_SIZE = 8704
    const val FILE_READ_BUFFER_SIZE = 1024 * 1024
}