package distributed.erasure.coding.pipeline

import distributed.erasure.coding.LRCErasureUtil
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.COORDINATOR_CHANNEL_NAME
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.HELPER_CHANNEL_PREFIX
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.JEDIS_POOL_MAX_SIZE
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.WORD_LENGTH
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.BLOCK_SIZE
import mu.KotlinLogging
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPubSub
import java.util.concurrent.CountDownLatch

open class Coordinator(
    val nodeHostMap: MutableMap<Int, Pair<String, Int>>,
    val blockNodeMap: MutableMap<String, Int>
) {
    private val logger = KotlinLogging.logger {}

    val COORDINATOR_IP = System.getProperty("coordinator.ip")
    val REDIS_PORT = 6379
    val erasureCode = ErasureCode.valueOf(System.getProperty("erasure.code"))

    val jedis: Jedis
    var pipelineUtil: PipelineUtil

    lateinit var latch: CountDownLatch

    init {
        // Initilize redis

        val jedisPoolConfig = JedisPoolConfig().apply { this.maxTotal = JEDIS_POOL_MAX_SIZE }
        val jedisPool = JedisPool(jedisPoolConfig, COORDINATOR_IP, REDIS_PORT)
        val jedisForSubscribe = jedisPool.resource
        jedis = jedisPool.resource
        pipelineUtil = PipelineUtil(jedis)

        val jedisPubSub = getJedisPubSub()

        Thread {
            jedisForSubscribe.psubscribe(jedisPubSub, "$COORDINATOR_CHANNEL_NAME.*", "$HELPER_CHANNEL_PREFIX.*", "REFRESH")
        }.start()

        logger.info("Initialized coordinator")
    }

    private fun getJedisPubSub() = object : JedisPubSub() {
        override fun onPMessage(pattern: String, channel: String, message: String) {
            logger.debug("Coordinator received channel: $channel, pattern message: $message")
            when (channel) {
                "$COORDINATOR_CHANNEL_NAME.fetch" -> fetchBlock(message)
                "$COORDINATOR_CHANNEL_NAME.terminated" -> waitForTerminate(message)
                "REFRESH" -> pipelineUtil = PipelineUtil(jedis)
            }
        }
    }

    open fun fetchBlock(message: String) {
        val split = message.split(" ")
        val requesterNodeId = split[0].toInt()
        val blockId = split[1]
        val erasedIndex = split[2].toInt()
        val fetchMethod = split[3]
        if (fetchMethod == "pipeline") {
            fetchBlockUsingPipelining(requesterNodeId, blockId, erasedIndex)
        } else {
            val senderNodeId = blockNodeMap[blockId] ?: -1
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
            logger.error("Error occurred while fetching block")
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

    open fun fetchBlockUsingPipelining(
        finalNodeId: Int,
        blockId: String,
        erasedIndex: Int
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
                val currBlockId = nodesPath[j - 1].second
                repairStripe(requesterNodeId, senderNodeId, currBlockId, i, j - 1, "invalid")
                senderNodeId = requesterNodeId
            }
            repairStripe(
                finalNodeId,
                senderNodeId,
                nodesPath[nodesPath.size - 1].second,
                i,
                nodesPath.size - 1,
                blockId
            )
        }
    }

    private fun repairStripe(
        requesterNodeId: Int,
        senderNodeId: Int,
        blockId: String,
        stripeIndex: Int,
        index: Int,
        endBlockId: String
    ) {
        val requesterHostPort = nodeHostMap[requesterNodeId]
        if (requesterHostPort == null) {
            logger.error("No node path found for repair pipelining stripe index: $stripeIndex")
        } else {
            val requesterHost = requesterHostPort.first
            val requesterPort = requesterHostPort.second
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$requesterNodeId.receive.pipeline.from",
                "$senderNodeId $blockId $stripeIndex $endBlockId"
            )
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$senderNodeId.send.pipeline.to",
                "$requesterHost $requesterPort $blockId $stripeIndex $index"
            )
        }
    }

    fun getNodesPathForLRC(blockId: String): List<Pair<Int, String>> {
        // Change return list using block -> node mapping

        val numBlocks = LRCErasureUtil.N
        val numDataBlocks = LRCErasureUtil.K
        val numGroupBlocks = LRCErasureUtil.R

        val startBlockIndex = (blockNodeMap.keys.indexOf(blockId) / (numGroupBlocks + 1)) * (numGroupBlocks + 1)
        val endBlockIndex =
            (blockNodeMap.keys.indexOf(blockId) / (numGroupBlocks + 1)) * (numGroupBlocks + 1) + numGroupBlocks

        // TODO: Change this logic
        logger.info("Block indices for local parity: $startBlockIndex..$endBlockIndex")

        val res = (startBlockIndex..endBlockIndex).filter { blockIndex ->
            // Ignore the block we need to fetch
            blockIndex != blockNodeMap.keys.indexOf(blockId)
        }.map { blockIndex ->
            val blockIdsInOrder = blockNodeMap.keys.toList()
            val currBlockId = blockIdsInOrder[blockIndex]
            val nodeId = blockNodeMap[currBlockId] ?: -1
            Pair(nodeId, currBlockId)
        }.toList()

        logger.debug("Nodes path: $res")
        return res
    }

    fun getNodesPath(blockId: String): List<Pair<Int, String>> {
        return listOf()
    }

    open fun waitForTerminate(message: String) {

    }
}

fun main(args: Array<String>) {
    val nodeHostMap = mutableMapOf(
        0 to Pair("127.0.0.1", 4444),
        1 to Pair("127.0.0.1", 7777),
        2 to Pair("127.0.0.1", 8888),
        3 to Pair("127.0.0.1", 9999)
    )

    // Blocks are stored in order
    val blockNodeMap = LinkedHashMap<String, Int>()
    blockNodeMap["0-LP.jpg"] = 0
    blockNodeMap["1-LP.jpg"] = 1
    blockNodeMap["2-LP.jpg"] = 2
    blockNodeMap["3-LP.jpg"] = 3

    blockNodeMap["4-LP.jpg"] = 0
    blockNodeMap["5-LP.jpg"] = 1
    blockNodeMap["6-LP.jpg"] = 2
    blockNodeMap["7-LP.jpg"] = 3
    val coordinator = Coordinator(nodeHostMap, blockNodeMap)

    while (true) {
        val latch = CountDownLatch(1)
        coordinator.latch = latch
        latch.await()
    }
}