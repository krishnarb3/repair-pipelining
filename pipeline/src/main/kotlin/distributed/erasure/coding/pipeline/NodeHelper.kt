package distributed.erasure.coding.pipeline

import distributed.erasure.coding.LRCErasureCode
import distributed.erasure.coding.pipeline.Util.BLOCK_SIZE
import distributed.erasure.coding.pipeline.Util.FILE_READ_BUFFER_SIZE
import distributed.erasure.coding.pipeline.Util.WORD_LENGTH
import java.io.*
import java.net.ServerSocket
import java.net.Socket

class NodeHelper {
    private var currBlockSendMap = mutableMapOf<String, Pair<DataInputStream, DataOutputStream>>()
    private var currBlockReceiveMap = mutableMapOf<String, Pair<DataInputStream, DataOutputStream>>()

    private val PORT_NUMBER = System.getenv("node.local.port").toInt()
    private val nodeId = System.getenv("node.local.id")

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
            System.err.println(exception.message)
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
            exception.printStackTrace()

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

            println("Node $nodeId going to send block: $blockId, stripe: $stripeIndex to port $requesterPortString")

            if (fileIn.available() > 0) {
                val bytes = fileIn.readNBytes(WORD_LENGTH)

                // Encode and send to next node
                lrcErasureCode.encodeParitySingle(bytes, currStripeData, encodeIndex, WORD_LENGTH)
                socketOut.write(currStripeData)
            }

            println("Node $nodeId sent block: $blockId, stripe: $stripeIndex to port $requesterPortString")
        } catch (exception: Exception) {
            exception.printStackTrace()
        } finally {
            if (stripeIndex.toInt() == BLOCK_SIZE / WORD_LENGTH - 1) {
                socketOut?.close()
                fileIn?.close()
                socket?.close()
            }
        }
    }

    fun receiveStripes(message: String) {
        val (senderNodeId, blockId, stripeIndex) = message.split(" ")
        var socket: Socket? = null
        var socketIn = currBlockReceiveMap[blockId]?.first
        var fileOut = currBlockReceiveMap[blockId]?.second

        println("Receiving stripe $stripeIndex for block $blockId")

        try {
            if (socketIn == null || fileOut == null) {
                socket = serverSocket.accept()
                println("Server $nodeId, opened socket at $PORT_NUMBER for accepting block: $blockId, stripe $stripeIndex")
                socketIn = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val file = File("receivedBlock-$nodeId.jpg")
                fileOut = DataOutputStream(FileOutputStream(file, true))
                currBlockReceiveMap[blockId] = Pair(socketIn, fileOut)
            }
            while (socketIn.available() <= 0) {
                // Wait for data to be available in socket
            }
            if (socketIn.available() > 0) {
                val data = socketIn.readNBytes(WORD_LENGTH)
                currStripeData = data
                fileOut.write(data)
                println("Written data for $stripeIndex")
            } else {
                println("No data available for $stripeIndex")
            }
        } catch (exception: Exception) {
            exception.printStackTrace()
        } finally {
            if (stripeIndex.toInt() == BLOCK_SIZE / WORD_LENGTH - 1) {
                println("Server $nodeId closed socket at $PORT_NUMBER opened for block $blockId after reaching stripe $stripeIndex")
                socketIn?.close()
                socket?.close()
                serverSocket.close()
                fileOut?.close()
            }
        }
    }
}