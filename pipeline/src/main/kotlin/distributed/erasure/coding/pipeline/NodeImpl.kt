package distributed.erasure.coding.pipeline

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPubSub
import java.io.*
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

class NodeImpl : Node {

    private val BLOCK_SIZE = 1024

    private val LOCAL_IP = System.getenv("node.local.ip")
    private val FILE_READ_BUFFER_SIZE = 1024 * 1024
    private val PORT_NUMBER = System.getenv("node.local.port").toInt()
    private val JEDIS_POOL_MAX_SIZE = System.getenv("jedis.pool.max.size").toInt()
    private val COORDINATOR_CHANNEL_NAME = "coordinator"
    private val HELPER_CHANNEL_PREFIX = "helper"
    private val nodeId = System.getenv("node.local.id")

    private var jedis: Jedis
    lateinit var _latch: CountDownLatch

    init {
        val jedisPoolConfig = JedisPoolConfig().apply { this.maxTotal = JEDIS_POOL_MAX_SIZE }
        val jedisPool = JedisPool(jedisPoolConfig, LOCAL_IP)
        val jedisForSubscribe = jedisPool.resource
        jedis = jedisPool.resource

        val jedisPubSub = getJedisPubSub()
        Thread {
            jedisForSubscribe.psubscribe(jedisPubSub, "$HELPER_CHANNEL_PREFIX.$nodeId.*")
        }.start()

        println("Node $nodeId initialized")
    }

    override fun repairAndPipeline(
        stripeIndex: Int,
        output: Array<ByteArray>,
        nodesInPipeline: List<Node>
    ) {

    }

    fun sendData(file: File, host: String, port: Int) {
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

    fun receiveData(message: String) {
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

    override fun fetchBlock(blockId: String) {
        jedis.publish(COORDINATOR_CHANNEL_NAME, "$nodeId $blockId")
    }

    override fun setLatch(latch: CountDownLatch) {
        this._latch = latch
    }

    private fun getJedisPubSub() = object : JedisPubSub() {
        override fun onPMessage(pattern: String, channel: String, message: String) {
            _latch.countDown()
            when (channel) {
                "$HELPER_CHANNEL_PREFIX.$nodeId.receive.from" -> receiveData(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.send.to" -> {
                    val requesterHost = message.split(" ")[0]
                    val requesterPort = message.split(" ")[1].toInt()
                    val requesterNodeId = message.split(" ")[2]
                    val blockId = message.split(" ")[3]
                    val file = File(blockId)
                    sendData(file, requesterHost, requesterPort)
                }
            }
        }
    }
}

fun main() {
    val node: Node = NodeImpl()
    val fetchCounter = AtomicInteger(0)

    while (true) {
        val latch = CountDownLatch(1)
        node.setLatch(latch)
        if (System.getenv("node.local.id").toInt() == 2 && fetchCounter.get() == 0) {
            node.fetchBlock("0-LP.jpg")
            fetchCounter.incrementAndGet()
        }
        latch.await()
    }

}