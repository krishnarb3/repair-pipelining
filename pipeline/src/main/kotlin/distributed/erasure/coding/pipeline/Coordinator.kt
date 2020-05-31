package distributed.erasure.coding.pipeline

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPubSub
import java.io.*
import java.util.concurrent.CountDownLatch

class Coordinator(
    val nodeHostMap: MutableMap<Int, Pair<String, Int>>,
    val blockNodeMap: MutableMap<String, Int>
) {
    private val BLOCK_SIZE = 1024 * 1024
    private val WORD_LENGTH = 1024

    private val COORDINATOR_CHANNEL_NAME = "coordinator"
    private val HELPER_CHANNEL_PREFIX = "helper"

    private val JEDIS_POOL_MAX_SIZE = System.getenv("jedis.pool.max.size").toInt()
    private val COORDINATOR_IP = System.getenv("coordinator.ip")
    private val REDIS_PORT = 6379

    private var jedis: Jedis

    lateinit var latch: CountDownLatch

    init {
        // Initilize redis

        val jedisPoolConfig = JedisPoolConfig().apply { this.maxTotal = JEDIS_POOL_MAX_SIZE }
        val jedisPool = JedisPool(jedisPoolConfig, COORDINATOR_IP, REDIS_PORT)
        val jedisForSubscribe = jedisPool.resource
        jedis = jedisPool.resource

        val jedisPubSub = getJedisPubSub()

        Thread {
            jedisForSubscribe.psubscribe(jedisPubSub, COORDINATOR_CHANNEL_NAME, "$HELPER_CHANNEL_PREFIX.*")
        }.start()

        println("Initialized coordinator")
    }

    private fun getJedisPubSub() = object : JedisPubSub() {
        override fun onPMessage(pattern: String, channel: String, message: String) {
            println("Coordinator received channel: $channel, pattern message: $message")
            when (channel) {
                COORDINATOR_CHANNEL_NAME -> fetchBlock(message)
            }
            latch.countDown()
        }
    }

    private fun fetchBlock(message: String) {
        val requesterNodeId = message.split(" ")[0].toInt()
        val requesterHost = nodeHostMap[requesterNodeId]
        val blockId = message.split(" ")[1]
        val senderNodeId = blockNodeMap[blockId] ?: -1
        fetchBlock(requesterNodeId, requesterHost, senderNodeId, blockId)
    }

    private fun fetchBlock(requesterNodeId: Int, requesterHostPort: Pair<String, Int>?, senderNodeId: Int, blockId: String) {
        if (requesterHostPort == null || senderNodeId == -1 || blockId == "") {
            System.err.println("Error occurred while fetching block")
            return
        }
        val requesterHost = requesterHostPort.first
        val requesterPort = requesterHostPort.second
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$requesterNodeId.receive.from",
            "$senderNodeId $blockId"
        )
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$senderNodeId.send.to",
            "$requesterHost $requesterPort $requesterNodeId $blockId"
        )
    }

    private fun fetchBlockUsingPipelining(requesterNodeId: Int, blockId: Int) {

        for (i in 0 until BLOCK_SIZE / WORD_LENGTH) {
            repairStripe(i)
        }
    }

    private fun repairStripe(stripeIndex: Int) {
        val nodesPath = getNodesPath(stripeIndex)

        val startNode = nodesPath.firstOrNull()

        if (startNode == null) {
            System.err.println("No node path found for repair pipelining stripe index: $stripeIndex")
        } else {
            // TODO: Change the output array
            val erasedWordsCount = 1
            val output = (0 until erasedWordsCount).map { ByteArray(WORD_LENGTH) }.toTypedArray()
            startNode.repairAndPipeline(stripeIndex, output, nodesPath)
        }
    }

    private fun getNodesPath(blockId: Int): List<Node> {
        return listOf()
    }
}

fun main() {
    val nodeHostMap = mutableMapOf(
        1 to Pair("127.0.0.1", 4444),
        2 to Pair("127.0.0.1", 7777)
    )
    val blockNodeMap = mutableMapOf(
        "0-LP.jpg" to 1,
        "1-LP.jpg" to 2
    )
    val coordinator = Coordinator(nodeHostMap, blockNodeMap)

    while (true) {
        val latch = CountDownLatch(1)
        coordinator.latch = latch
        latch.await()
    }

}