package distributed.erasure.coding.pipeline

import distributed.erasure.coding.LRCErasureCode
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.BLOCK_SIZE
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.FILE_READ_BUFFER_SIZE
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.WORD_LENGTH
import mu.KotlinLogging
import java.io.*
import java.net.ServerSocket
import java.net.Socket

class NodeHelper {
    private val logger = KotlinLogging.logger {}

    private var currBlockSendMap = mutableMapOf<String, Pair<DataInputStream, DataOutputStream>>()
    private var currBlockReceiveMap = mutableMapOf<String, Pair<DataInputStream, DataOutputStream>>()

    private val PORT_NUMBER = System.getProperty("node.local.port").toInt()
    private val nodeId = System.getProperty("node.local.id")

    private val lrcErasureCode = LRCErasureCode()
    private val serverSocket = ServerSocket(PORT_NUMBER)
    private var currStripeData = ByteArray(WORD_LENGTH)

    fun sendBlock(file: File, host: String, port: Int) {
        var socket: Socket? = null
        var socketOut: DataOutputStream? = null
        var fileIn: DataInputStream? = null

        try {
            socket = Socket(host, port)
            socketOut = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            fileIn = DataInputStream(BufferedInputStream(file.inputStream()))

            while (fileIn.available() > 0) {
                val bytes = fileIn.readNBytes(FILE_READ_BUFFER_SIZE)
                socketOut.write(bytes)
            }
        } catch (exception: Exception) {
            logger.error(exception.message)
        } finally {
            socketOut?.close()
            fileIn?.close()
            socket?.close()
        }
    }

    fun receiveBlock(message: String) {
        var socket: Socket? = null
        var socketIn: DataInputStream? = null
        try {
            socket = serverSocket.accept()
            socketIn = DataInputStream(BufferedInputStream(socket.getInputStream()))
            while (socketIn.available() > 0) {
                val data = socketIn.readAllBytes()
                DataOutputStream(File("receivedBlock.jpg").outputStream()).use {
                    it.write(data)
                }
            }
        } catch (exception: Exception) {
            logger.error { exception.stackTrace }
        }
    }

    fun sendStripes(message: String) {
        val (requesterHost, requesterPortString, blockId, stripeIndex, encodeIndexString) = message.split(" ")

        val encodeIndex = encodeIndexString.toInt()
        var socket: Socket? = null
        var fileIn = currBlockSendMap[blockId]?.first
        var socketOut = currBlockSendMap[blockId]?.second

        try {
            if (socketOut == null || fileIn == null) {
                socket = Socket(requesterHost, requesterPortString.toInt())
                socketOut = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))

                val file = File(blockId)
                fileIn = DataInputStream(BufferedInputStream(file.inputStream()))
                currBlockSendMap[blockId] = Pair(fileIn, socketOut)
            }

            logger.debug("Node $nodeId going to send block: $blockId, stripe: $stripeIndex to port $requesterPortString")

            if (fileIn.available() > 0) {
                val bytes = fileIn.readNBytes(WORD_LENGTH)

                // Encode and send to next node
                lrcErasureCode.encodeParitySingle(bytes, currStripeData, encodeIndex, WORD_LENGTH)
                socketOut.write(currStripeData)
            }

            logger.debug("Node $nodeId sent block: $blockId, stripe: $stripeIndex to port $requesterPortString")
        } catch (exception: Exception) {
            logger.error { exception.stackTrace }
        } finally {
            if (stripeIndex.toInt() == BLOCK_SIZE / WORD_LENGTH - 1) {
                socketOut?.close()
                fileIn?.close()
                socket?.close()
            }
        }
    }

    fun receiveStripes(message: String) {
        val (senderNodeId, blockId, stripeIndex, endBlockId) = message.split(" ")
        var socket: Socket? = null
        var socketIn = currBlockReceiveMap[blockId]?.first
        var fileOut = currBlockReceiveMap[blockId]?.second

        logger.debug("Receiving stripe $stripeIndex for block $blockId")

        try {
            if (socketIn == null || fileOut == null) {
                socket = serverSocket.accept()
                logger.info("Server $nodeId, opened socket at $PORT_NUMBER for accepting block: $blockId, stripe $stripeIndex")
                socketIn = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val file = File(endBlockId)
                fileOut = DataOutputStream(FileOutputStream(file, true))
                currBlockReceiveMap[blockId] = Pair(socketIn, fileOut)
            }
            while (socketIn.available() <= 0) {
                // Wait for data to be available in socket
            }
            if (socketIn.available() > 0) {
                val data = socketIn.readNBytes(WORD_LENGTH)
                currStripeData = data
                if (endBlockId != "invalid") {
                    fileOut.write(data)
                }
                logger.debug("Written data for $stripeIndex")
            } else {
                logger.debug("No data available for $stripeIndex")
            }
        } catch (exception: Exception) {
            logger.error { exception.stackTrace }
        } finally {
            if (stripeIndex.toInt() == BLOCK_SIZE / WORD_LENGTH - 1) {
                logger.info("Server $nodeId closed socket at $PORT_NUMBER opened for block $blockId after reaching stripe $stripeIndex")
                socketIn?.close()
                socket?.close()
                serverSocket.close()
                fileOut?.close()
            }
        }
    }
}