package distributed.erasure.coding.pipeline

data class Block(val words: Array<Word>, val blockSize: Int) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Block

        if (!words.contentDeepEquals(other.words)) return false
        if (blockSize != other.blockSize) return false

        return true
    }

    override fun hashCode(): Int {
        var result = words.contentDeepHashCode()
        result = 31 * result + blockSize
        return result
    }
}

// index should be the same across blocks for all words belonging to a stripe
data class Word(val data: ByteArray, val index: Int) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Word

        if (!data.contentEquals(other.data)) return false
        if (index != other.index) return false

        return true
    }

    override fun hashCode(): Int {
        var result = data.contentHashCode()
        result = 31 * result + index
        return result
    }
}