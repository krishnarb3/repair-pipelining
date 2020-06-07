package distributed.erasure.coding.pipeline

object Util {
    val BLOCK_SIZE = 34816
    val WORD_LENGTH = BLOCK_SIZE / 1024
    val FILE_READ_BUFFER_SIZE = 1024 * 1024
}