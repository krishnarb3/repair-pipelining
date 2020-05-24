package distributed.erasure.coding

import com.backblaze.erasure.ReedSolomon
import java.io.*

fun main() {
    val prefix = "/Users/rb"

    val inputFileName = "$prefix/Downloads/LP.jpg"
    val encodeOutputSuffix = "LP.jpg"
    val outputFileName = "$prefix/Desktop/LP-copy.jpg"

    encode(inputFileName, encodeOutputSuffix)
    decode(inputFileName, outputFileName, encodeOutputSuffix, listOf())

    println("Done")
}

fun encode(inputFileName: String, outputFileSuffix: String) {
    val file = File(inputFileName)
    val reader = DataInputStream(BufferedInputStream(file.inputStream()))

    val len = file.length()
    val blockSize = len / ErasureUtil.K

    val rs = ReedSolomon.create(ErasureUtil.R, 1)

    for (i in 0 until ErasureUtil.K / ErasureUtil.R) {
        val shards = Array<ByteArray>(ErasureUtil.R + 1) { ByteArray(blockSize.toInt()) }
        for (j in 0 until ErasureUtil.R) {
            val bytesRead = reader.readNBytes(blockSize.toInt())
            shards[j] = bytesRead
        }
        rs.encodeParity(shards, 0, blockSize.toInt())

        for (j in 0 until ErasureUtil.R + 1) {
            val outputFile = File("${i * ErasureUtil.R + j}-$outputFileSuffix")
            val writer = DataOutputStream(BufferedOutputStream(outputFile.outputStream()))
            writer.write(shards[j])
            writer.close()
        }
    }

    reader.close()

}

fun decode(inputFileName: String, outputFileName: String, outputFileSuffix: String, missingIndices: List<Int>) {
    val inputFile = File(inputFileName)
    val blockSize = inputFile.length() / ErasureUtil.K
    val rs = ReedSolomon.create(ErasureUtil.R, 1)

    val shards = Array<ByteArray>(ErasureUtil.N) { ByteArray(blockSize.toInt()) }

    for (i in 0 until ErasureUtil.N / ErasureUtil.R) {
        for (j in 0 until ErasureUtil.R + 1) {
            val index = i * ErasureUtil.R + j
            val file = File("$index-$outputFileSuffix")
            if (!missingIndices.contains(index) && file.exists()) {
                val inputStream = DataInputStream(BufferedInputStream(file.inputStream()))
                shards[index] = inputStream.readAllBytes()
                inputStream.close()
            }
        }
        val shardsPresent = (i until i + ErasureUtil.R + 1).map { !missingIndices.contains(it) }.toBooleanArray()
        rs.decodeMissing(shards.sliceArray(i until i + ErasureUtil.R + 1), shardsPresent, 0, blockSize.toInt())
    }

    val outputFile = File(outputFileName)
    val writer = DataOutputStream(BufferedOutputStream(outputFile.outputStream()))
    shards.mapIndexed { index, bytes ->
        if (index == 0 || (index + 1) % ErasureUtil.R + 1 != 0) {
            writer.write(bytes)
        }
    }
    writer.close()
}