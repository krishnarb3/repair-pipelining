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

class NodeImpl(val nodeHelper: NodeHelper) : Node {

    private val BLOCK_SIZE = 1024

    private val LOCAL_IP = System.getenv("node.local.ip")
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
                "$HELPER_CHANNEL_PREFIX.$nodeId.receive.from" -> nodeHelper.receiveBlock(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.send.to" -> {
                    val requesterHost = message.split(" ")[0]
                    val requesterPort = message.split(" ")[1].toInt()
                    val requesterNodeId = message.split(" ")[2]
                    val blockId = message.split(" ")[3]
                    val file = File(blockId)
                    nodeHelper.sendBlock(file, requesterHost, requesterPort)
                }

                "$HELPER_CHANNEL_PREFIX.$nodeId.receive.pipeline.from" -> nodeHelper.receiveStripes(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.send.pipeline.to" -> nodeHelper.sendStripes(message)
            }
        }
    }
}

fun main() {
    val nodeHelper = NodeHelper()
    val node: Node = NodeImpl(nodeHelper)
    val fetchCounter = AtomicInteger(0)

    while (true) {
        val latch = CountDownLatch(1)
        node.setLatch(latch)
        if (System.getenv("node.local.id").toInt() == 3 && fetchCounter.get() == 0) {
            node.fetchBlock("3-LP.jpg")
            fetchCounter.incrementAndGet()
        }
        latch.await()
    }

}