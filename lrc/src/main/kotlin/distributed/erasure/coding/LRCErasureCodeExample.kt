package distributed.erasure.coding

import com.backblaze.erasure.ReedSolomon
import java.io.*
import java.util.*


fun main() {
    val prefix = "."

    val inputFileName = "$prefix/LP-block.jpg"
    val encodeOutputSuffix = "LP.jpg"
    val outputFileName = "$prefix/LP-copy.jpg"

    val scanner = Scanner(System.`in`)
    println("Please enter option: 1. Encode using all inputs 2. Encode using one-by-one inputs 3. Skip encode")

    val option = scanner.nextInt()
    when (option) {
        1 -> encode(inputFileName, encodeOutputSuffix)
        2 -> encodeUsingSingle(inputFileName, encodeOutputSuffix)
        3 -> {}
    }

    decode(inputFileName, outputFileName, encodeOutputSuffix, listOf(2))

    println("Done")
}

fun encode(inputFileName: String, outputFileSuffix: String) {
    val file = File(inputFileName)
    val reader = DataInputStream(BufferedInputStream(file.inputStream()))

    val len = file.length()
    val blockSize = len / LRCErasureUtil.K

    val rs = ReedSolomon.create(LRCErasureUtil.R, 1)

    for (i in 0 until LRCErasureUtil.K / LRCErasureUtil.R) {
        val shards = Array<ByteArray>(LRCErasureUtil.R + 1) { ByteArray(blockSize.toInt()) }
        for (j in 0 until LRCErasureUtil.R) {
            val bytesRead = reader.readNBytes(blockSize.toInt())
            shards[j] = bytesRead
        }
        rs.encodeParity(shards, 0, blockSize.toInt())

        for (j in 0 until LRCErasureUtil.R + 1) {
            val outputFile = File("${i * (LRCErasureUtil.R+1) + j}-$outputFileSuffix")
            val writer = DataOutputStream(BufferedOutputStream(outputFile.outputStream()))
            writer.write(shards[j])
            writer.close()
        }
    }

    reader.close()

}

fun encodeUsingSingle(inputFileName: String, outputFileSuffix: String) {
    val file = File(inputFileName)
    val reader = DataInputStream(BufferedInputStream(file.inputStream()))

    val len = file.length()
    val blockSize = len / LRCErasureUtil.K

    val rs = ReedSolomon.create(LRCErasureUtil.R, 1)

    for (i in 0 until LRCErasureUtil.K / LRCErasureUtil.R) {
        val shards = Array<ByteArray>(LRCErasureUtil.R + 1) { ByteArray(blockSize.toInt()) }
        for (j in 0 until LRCErasureUtil.R) {
            val bytesRead = reader.readNBytes(blockSize.toInt())
            shards[j] = bytesRead
        }

        val output = ByteArray(shards[0].size)
        for (index in (0 until shards.size - 1)) {
            rs.encodeParitySingle(shards[index], output, index, 0, 0, blockSize.toInt())
        }
        shards[shards.size - 1] = output

        for (j in 0 until LRCErasureUtil.R + 1) {
            val outputFile = File("${i * (LRCErasureUtil.R+1) + j}-$outputFileSuffix")
            val writer = DataOutputStream(BufferedOutputStream(outputFile.outputStream()))
            writer.write(shards[j])
            writer.close()
        }
    }

    reader.close()
}

fun decode(inputFileName: String, outputFileName: String, outputFileSuffix: String, missingIndices: List<Int>) {
    val inputFile = File(inputFileName)
    val blockSize = inputFile.length() / LRCErasureUtil.K
    val rs = ReedSolomon.create(LRCErasureUtil.R, 1)

    val shards = Array<ByteArray>(LRCErasureUtil.N) { ByteArray(blockSize.toInt()) }

    for (i in 0 until LRCErasureUtil.K / LRCErasureUtil.R) {
        for (j in 0 until LRCErasureUtil.R + 1) {
            val index = (i * (LRCErasureUtil.R + 1)) + j
            val file = File("$index-$outputFileSuffix")
            if (!missingIndices.contains(index) && file.exists()) {
                val inputStream = DataInputStream(BufferedInputStream(file.inputStream()))
                shards[index] = inputStream.readAllBytes()
                inputStream.close()
            } else {
                println("Missing index: $index")
            }
        }
        val sliceStart = i * (LRCErasureUtil.R + 1)
        val sliceEnd = i * (LRCErasureUtil.R + 1) + LRCErasureUtil.R + 1
        val shardsPresent = (sliceStart until sliceEnd).map { !missingIndices.contains(it) }.toBooleanArray()
        val shardSlice = shards.sliceArray(sliceStart until sliceEnd)
        rs.decodeMissing(shardSlice, shardsPresent, 0, blockSize.toInt())

        var counter = 0
        for (entry in shardSlice) {
            shards[sliceStart + counter++] = entry
        }
    }

    val outputFile = File(outputFileName)
    val writer = DataOutputStream(BufferedOutputStream(outputFile.outputStream()))
    shards.mapIndexed { index, bytes ->
        if (index == 0 || (index + 1) % (LRCErasureUtil.R + 1) != 0) {
            writer.write(bytes)
        }
    }
    writer.close()
}