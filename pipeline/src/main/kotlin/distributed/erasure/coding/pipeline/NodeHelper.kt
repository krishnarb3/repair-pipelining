package distributed.erasure.coding.pipeline

import distributed.erasure.coding.pipeline.Util.BLOCK_SIZE
import distributed.erasure.coding.pipeline.Util.FILE_READ_BUFFER_SIZE
import distributed.erasure.coding.pipeline.Util.WORD_LENGTH
import java.io.*
import java.net.ServerSocket
import java.net.Socket

class NodeHelper {
    private var currBlockOutputStreamMap = mutableMapOf<String, DataOutputStream>()
    private var currBlockInputStreamMap = mutableMapOf<String, DataInputStream>()

    private val PORT_NUMBER = System.getenv("node.local.port").toInt()

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
        var serverSocket: ServerSocket? = null
        var socket: Socket? = null
        var socketIn: DataInputStream? = null
        try {
            serverSocket = ServerSocket(PORT_NUMBER)
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
        val (requesterHost, requesterPortString, blockId, stripeIndex) = message.split(" ")
        var socket: Socket? = null
        var socketOut = currBlockOutputStreamMap[blockId]
        var fileIn: DataInputStream? = null

        // TODO: Change file
        val file = File(blockId)

        try {
            socket = Socket(requesterHost, requesterPortString.toInt())
            if (socketOut == null) {
                socketOut = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                currBlockOutputStreamMap[blockId] = socketOut
            }
            fileIn = DataInputStream(BufferedInputStream(file.inputStream()))

            while (fileIn.available() > 0) {
                val bytes = fileIn.readNBytes(FILE_READ_BUFFER_SIZE)
                socketOut.write(bytes)
            }
        } catch (exception: Exception) {
            System.err.println(exception.message)
        } finally {
            if (stripeIndex.toInt() == BLOCK_SIZE / WORD_LENGTH - 1) {
                socketOut?.close()
            }
            fileIn?.close()
            socket?.close()
        }
    }

    fun receiveStripes(message: String) {
        val (senderNodeId, blockId, stripeIndex) = message.split(" ")
        var serverSocket: ServerSocket? = null
        var socket: Socket? = null
        var socketIn = currBlockInputStreamMap[blockId]
        try {
            serverSocket = ServerSocket(PORT_NUMBER)
            socket = serverSocket.accept()
            if (socketIn == null) {
                socketIn = DataInputStream(BufferedInputStream(socket.getInputStream()))
                currBlockInputStreamMap[blockId] = socketIn
            }
            while (socketIn.available() > 0) {
                val data = socketIn.readAllBytes()
                DataOutputStream(File("receivedBlock.jpg").outputStream()).use {
                    it.write(data)
                }
            }
        } catch (exception: Exception) {
            exception.printStackTrace()
        } finally {
            if (stripeIndex.toInt() == BLOCK_SIZE / WORD_LENGTH - 1) {
                socketIn?.close()
            }
            socket?.close()
            serverSocket?.close()
        }
    }
}