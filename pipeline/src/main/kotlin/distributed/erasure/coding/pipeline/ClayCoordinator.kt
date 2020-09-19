package distributed.erasure.coding.pipeline

import distributed.erasure.coding.clay.ClayCode
import distributed.erasure.coding.clay.ClayCodeErasureDecodingStep
import distributed.erasure.coding.clay.ECBlock
import distributed.erasure.coding.pipeline.Util.CLAY_BLOCK_SIZE
import distributed.erasure.coding.pipeline.Util.HELPER_CHANNEL_PREFIX
import distributed.erasure.coding.pipeline.Util.NUM_DATA_UNITS
import distributed.erasure.coding.pipeline.Util.NUM_PARITY_UNITS
import distributed.erasure.coding.pipeline.Util.NUM_TOTAL_UNITS
import distributed.erasure.coding.pipeline.Util.SUBPACKET_SIZE
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch

class ClayCoordinator(
    nodeHostMap: MutableMap<Int, Pair<String, Int>>,
    blockNodeMap: MutableMap<String, Int>,
    val blockIndexMap: MutableMap<String, Int>
) : Coordinator(nodeHostMap, blockNodeMap) {

    private val logger = KotlinLogging.logger {}
    private var prevTime: Long = 0L
    private val terminatedMap = mutableMapOf<String, MutableMap<Int, CompletableFuture<Int>>>()

    var inputs = Array((NUM_DATA_UNITS + NUM_PARITY_UNITS) * SUBPACKET_SIZE) {
        ECBlock(false, false)
    }

    override fun fetchBlockUsingPipelining(finalNodeId: Int, blockId: String, erasedIndex: Int) {
        fetchBlockUsingPipelining(finalNodeId, erasedIndex, blockId)
    }

    fun fetchBlockUsingPipelining(finalNodeId: Int, erasedIndex: Int, blockId: String) {
        val erasedIndexes = intArrayOf(erasedIndex)
        val isDirect = true
        val util = ClayCodeErasureDecodingStep.ClayCodeUtil(
            erasedIndexes, NUM_DATA_UNITS, NUM_PARITY_UNITS
        )
        val parityIndexes = (NUM_DATA_UNITS until NUM_TOTAL_UNITS).toList().toIntArray()
        val clayCode = ClayCode(NUM_DATA_UNITS, NUM_PARITY_UNITS, CLAY_BLOCK_SIZE, parityIndexes)
        val originalInputs = clayCode.getInputs()
        val originalOutputs = clayCode.getOutputs()
        val encodedResult = clayCode.encode(originalInputs, originalOutputs)

        inputs = clayCode.getTestInputs(encodedResult[0], encodedResult[1], erasedIndexes)

        for (i in 0 until NUM_TOTAL_UNITS) {
            val futures = terminatedMap[blockId] ?: mutableMapOf()
            futures[i] = CompletableFuture()
            terminatedMap[blockId] = futures
        }

        prevTime = System.nanoTime()
        logger.info("Starting decode")

        decode(util, blockId, erasedIndex)
    }

    override fun waitForTerminate(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val nodeId = split[1].toInt()

        logger.debug("Node $nodeId terminated")
        terminatedMap[blockId]?.get(nodeId)?.complete(nodeId)

        val blockCompleted = terminatedMap[blockId]?.values?.all { fut -> fut.isDone } ?: false

        if (blockCompleted) {
            val currentTime = System.nanoTime()
            val timeElapsed = (currentTime - prevTime)/1000000000.0
            logger.info("Completed fetch block: $blockId using pipelining")
            logger.info("Time taken: $timeElapsed")
        }
    }

    fun decode(
        util: ClayCodeErasureDecodingStep.ClayCodeUtil,
        blockId: String,
        erasedIndex: Int
    ) {
        val helperIndexes = util.getHelperPlanesIndexes(erasedIndex)

        for (i in helperIndexes.indices) {
            val z = helperIndexes[i]
            val z_vec = util.getZVector(z)

            getAndStoreDecoupledData(blockId, util, helperIndexes, i, erasedIndex, z_vec)

            val erasedDecoupledNodes = IntArray(util.q)
            val y = util.getNodeCoordinates(erasedIndex)[1]
            for (x in 0 until util.q) {
                erasedDecoupledNodes[x] = util.getNodeIndex(x, y)
            }
            logger.debug("Erased decoupled nodes: ${erasedDecoupledNodes.joinToString(",")}")

            // Currently running into a race condition issue without some delay
            Thread.sleep(1000)

            decodeDecoupledData(blockId, erasedDecoupledNodes, helperIndexes[i])

            getAndStoreErasedData(blockId, util, erasedIndex, i, helperIndexes, y)
        }

        for (i in 0 until util.q * util.t) {
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$i.terminate",
                blockId
            )
        }
    }

    private fun getAndStoreDecoupledData(
        blockId: String,
        util: ClayCodeErasureDecodingStep.ClayCodeUtil,
        helperIndexes: IntArray,
        currHelperIndex: Int,
        erasedIndex: Int,
        z_vec: IntArray
    ) {
        val erasedCoordinates = util.getNodeCoordinates(erasedIndex)
        for (i in 0 until  util.q * util.t) {
            val coordinates = util.getNodeCoordinates(i)
            if (coordinates[1] != erasedCoordinates[1]) {
                if (z_vec[coordinates[1]] == coordinates[0]) {
                    storeDecoupledData(blockId, i, helperIndexes[currHelperIndex])
                } else {
                    val z = helperIndexes[currHelperIndex]
                    val coupleZIndex = util.getCouplePlaneIndex(coordinates, z)
                    val coupleHelperPlaneIndex = helperIndexes.firstOrNull { it == coupleZIndex }
                        ?: throw Exception("Couple helper plane index not found")
                    val coupleCoordinates = util.getNodeIndex(z_vec[coordinates[1]], coordinates[1])
                    sendAndStoreDecoupledData(
                        blockId = blockId,
                        receiverNode = i,
                        receiverSubpacketIndex = helperIndexes[currHelperIndex],
                        senderNode = coupleCoordinates,
                        senderSubpacketIndex = coupleHelperPlaneIndex
                    )
                }
            }
        }
    }

    private fun storeDecoupledData(blockId: String, nodeIndex: Int, subpacketIndex: Int) {
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$nodeIndex.store.decoupled.data",
            "$blockId $subpacketIndex $CLAY_BLOCK_SIZE"
        )
    }

    private fun sendAndStoreDecoupledData(
        blockId: String,
        receiverNode: Int,
        receiverSubpacketIndex: Int,
        senderNode: Int,
        senderSubpacketIndex: Int
    ) {
        val receiverHost = nodeHostMap[receiverNode]?.first
            ?: throw Exception("Host details not found for $receiverNode")
        val receiverPort = nodeHostMap[receiverNode]?.second
            ?: throw Exception("Host details not found for $receiverNode")
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$receiverNode.receive.decoupled.data",
            "$blockId $receiverSubpacketIndex $CLAY_BLOCK_SIZE"
        )
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$senderNode.send.data.for.decouple",
            "$blockId $senderSubpacketIndex $CLAY_BLOCK_SIZE $receiverHost $receiverPort"
        )
    }

    private fun decodeDecoupledData(
        blockId: String,
        erasedDecoupledNodes: IntArray,
        subpacketIndex: Int
    ) {
        val nodesPath = getNodesPath(blockId, erasedDecoupledNodes)
        logger.debug("NodesPath: ${nodesPath.joinToString(",")}")
        for (i in 0 until nodesPath.size - 1) {
            val receiverId = nodesPath[i+1]
            val senderId = nodesPath[i]
            val receiverHost = nodeHostMap[receiverId]?.first ?: throw Exception("Host not found for $receiverId")
            val receiverPort = nodeHostMap[receiverId]?.second ?: throw Exception("Port not found for $receiverId")
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$receiverId.receive.output.data",
                "$blockId $subpacketIndex $CLAY_BLOCK_SIZE ${erasedDecoupledNodes.size}"
            )
            val first = i == 0
            val message = "$blockId $subpacketIndex $CLAY_BLOCK_SIZE" +
                    " $receiverHost $receiverPort $i" +
                    " $NUM_DATA_UNITS $NUM_PARITY_UNITS" +
                    " ${erasedDecoupledNodes.joinToString(",")} $first"
            jedis.publish("$HELPER_CHANNEL_PREFIX.$senderId.decode.and.send", message)
        }

        // Send to erased nodes
        val lastNodeId = nodesPath.last()
        val receiverHosts = mutableListOf<String>()
        val receiverPorts = mutableListOf<Int>()
        val outputIndexes = mutableListOf<Int>()
        for (index in erasedDecoupledNodes.indices) {
            val receiverId = erasedDecoupledNodes[index]
            receiverHosts.add(nodeHostMap[receiverId]?.first ?: throw Exception("Host not found for $receiverId"))
            receiverPorts.add(nodeHostMap[receiverId]?.second ?: throw Exception("Port not found for $receiverId"))
            outputIndexes.add(index)
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$receiverId.receive.decoded.data",
                "$blockId $subpacketIndex $CLAY_BLOCK_SIZE"
            )
        }
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$lastNodeId.send.decoded.data",
            "$blockId $subpacketIndex $CLAY_BLOCK_SIZE" +
                    " ${receiverHosts.joinToString(",")} ${receiverPorts.joinToString(",")}" +
                    " ${nodesPath.lastIndex} ${outputIndexes.joinToString(",")}" +
                    " $NUM_DATA_UNITS $NUM_PARITY_UNITS" +
                    " ${erasedDecoupledNodes.joinToString(",")} false"
        )
    }

    private fun getAndStoreErasedData(
        blockId: String,
        util: ClayCodeErasureDecodingStep.ClayCodeUtil,
        erasedIndex: Int,
        currHelperIndex: Int,
        helperIndexes: IntArray,
        y: Int
    ) {
        for (x in 0 until util.q) {
            val z = helperIndexes[currHelperIndex]
            val nodeIndex = util.getNodeIndex(x, y)

            if (nodeIndex == erasedIndex) {
                storeErasedData(blockId, nodeIndex, helperIndexes[currHelperIndex])
            } else {
                val coupledZIndex = util.getCouplePlaneIndex(intArrayOf(x, y), z)

                sendErasedData(blockId, nodeIndex, helperIndexes[currHelperIndex], helperIndexes[currHelperIndex], coupledZIndex, erasedIndex, nodeHostMap[erasedIndex]!!.first, nodeHostMap[erasedIndex]!!.second)
            }
        }
    }

    private fun storeErasedData(blockId: String, nodeIndex: Int, subpacketIndex: Int) {
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$nodeIndex.store.erased.data",
            "$blockId $subpacketIndex $CLAY_BLOCK_SIZE"
        )
    }

    private fun sendErasedData(
        blockId: String, nodeIndex: Int, coupleSubpacketIndex: Int, decoupleSubpacketIndex: Int,
        zIndex: Int,
        receiverId: Int, receiverHost: String, receiverPort: Int
    ) {
        logger.debug("Node $nodeIndex sending erased data to $receiverId")
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$receiverId.receive.erased.data",
            "$blockId $zIndex $CLAY_BLOCK_SIZE"
        )
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$nodeIndex.send.erased.data",
            "$blockId $coupleSubpacketIndex $decoupleSubpacketIndex $CLAY_BLOCK_SIZE $receiverHost $receiverPort"
        )
    }

    private fun getNodesPath(blockId: String, erasedNodes: IntArray): List<Int> {
        return (0 until NUM_TOTAL_UNITS).filterNot { erasedNodes.contains(it) }
    }
}

fun main() {
    val nodeHostMap = mutableMapOf(
        0 to Pair("127.0.0.1", 1111),
        1 to Pair("127.0.0.1", 2222),
        2 to Pair("127.0.0.1", 3333),
        3 to Pair("127.0.0.1", 4444),
        4 to Pair("127.0.0.1", 5555),
        5 to Pair("127.0.0.1", 6666)
    )
    val blockNodeMap = LinkedHashMap<String, Int>()
    val blockIndexMap = mutableMapOf<String, Int>()

    for (i in 0 until NUM_TOTAL_UNITS) {
        for (j in 0 until SUBPACKET_SIZE) {
            blockNodeMap["$i $j"] = (i * SUBPACKET_SIZE + j) % NUM_TOTAL_UNITS
            blockIndexMap["$i $j"] = (i * SUBPACKET_SIZE + j)
        }
    }

    val coordinator = ClayCoordinator(nodeHostMap, blockNodeMap, blockIndexMap)

    while (true) {
        val latch = CountDownLatch(1)
        coordinator.latch = latch
        latch.await()
    }
}