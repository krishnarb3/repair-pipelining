package distributed.erasure.coding.pipeline

import distributed.erasure.coding.LRCErasureUtil
import distributed.erasure.coding.pipeline.Util.BLOCK_SIZE
import distributed.erasure.coding.pipeline.Util.WORD_LENGTH
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPubSub
import java.util.concurrent.CountDownLatch

class Coordinator(
    val nodeHostMap: MutableMap<Int, Pair<String, Int>>,
    val blockNodeMap: MutableMap<String, Int>
) {
    private val COORDINATOR_CHANNEL_NAME = "coordinator"
    private val HELPER_CHANNEL_PREFIX = "helper"

    private val JEDIS_POOL_MAX_SIZE = System.getenv("jedis.pool.max.size").toInt()
    private val COORDINATOR_IP = System.getenv("coordinator.ip")
    private val REDIS_PORT = 6379
    private val erasureCode = ErasureCode.valueOf(System.getenv("erasure.code"))
    private val fetchMethod = System.getenv("fetch.method") ?: "normal"

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
        val blockId = message.split(" ")[1]
        val senderNodeId = blockNodeMap[blockId] ?: -1
        if (fetchMethod == "pipeline") {
            fetchBlockUsingPipelining(requesterNodeId, blockId)
        } else {
            fetchBlock(requesterNodeId, senderNodeId, blockId)
        }
    }

    private fun fetchBlock(
        requesterNodeId: Int,
        senderNodeId: Int,
        blockId: String
    ) {
        val requesterHostPort = nodeHostMap[requesterNodeId]
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

    private fun fetchBlockUsingPipelining(
        finalNodeId: Int,
        blockId: String
    ) {
        val nodesPath = when (erasureCode) {
            ErasureCode.LRC -> getNodesPathForLRC(blockId)
            else -> getNodesPath(blockId)
        }

        if (nodesPath.size < 2) {
            return
        }

        for (i in 0 until BLOCK_SIZE / WORD_LENGTH) {
            var senderNodeId = nodesPath[0].first
            var requesterNodeId = nodesPath[1].first
            for (j in 1 until nodesPath.size) {
                requesterNodeId = nodesPath[j].first
                val currBlockId = nodesPath[j].second
                repairStripe(requesterNodeId, senderNodeId, currBlockId, i)
                senderNodeId = requesterNodeId
            }
        }
    }

    private fun repairStripe(
        requesterNodeId: Int,
        senderNodeId: Int,
        blockId: String,
        stripeIndex: Int
    ) {
        val requesterHostPort = nodeHostMap[requesterNodeId]
        if (requesterHostPort == null) {
            System.err.println("No node path found for repair pipelining stripe index: $stripeIndex")
        } else {
            val requesterHost = requesterHostPort.first
            val requesterPort = requesterHostPort.second
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$requesterNodeId.receive.pipeline.from",
                "$senderNodeId $blockId $stripeIndex"
            )
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$senderNodeId.send.pipeline.to",
                "$requesterHost $requesterPort $blockId $stripeIndex"
            )
        }
    }

    private fun getNodesPathForLRC(blockId: String): List<Pair<Int, String>> {
        // Change return list using block -> node mapping

        val numBlocks = LRCErasureUtil.N
        val numDataBlocks = LRCErasureUtil.K
        val numGroupBlocks = LRCErasureUtil.R

        val startNodeId = blockId.toInt() / (numGroupBlocks + 1)
        val endNodeId = startNodeId + numGroupBlocks

        return (startNodeId..endNodeId).filter { it != blockId.toInt() }.map { Pair(it, it.toString()) }.toList()
    }

    private fun getNodesPath(blockId: String): List<Pair<Int, String>> {
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