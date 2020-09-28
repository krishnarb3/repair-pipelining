package distributed.erasure.coding.pipeline

import distributed.erasure.coding.clay.*
import distributed.erasure.coding.pipeline.PipelineUtil.Companion.HELPER_CHANNEL_PREFIX
import mu.KotlinLogging
import redis.clients.jedis.StreamEntryID
import java.io.*
import java.net.ServerSocket
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch

class ClayCoordinator(
    nodeHostMap: MutableMap<Int, Pair<String, Int>>,
    blockNodeMap: MutableMap<String, Int>,
    val blockIndexMap: MutableMap<String, Int>
) : Coordinator(nodeHostMap, blockNodeMap) {

    private val PORT_NUMBER = System.getProperty("coordinator.local.port").toInt()
    private val serverSocket = ServerSocket(PORT_NUMBER)

    private val logger = KotlinLogging.logger {}
    private var prevTime: Long = 0L
    private val terminatedMap = mutableMapOf<String, MutableMap<Int, CompletableFuture<Int>>>()

    init {
        for (i in 0 until pipelineUtil.numTotalUnits) {
            for (j in 0 until pipelineUtil.subpacketSize) {
                blockNodeMap["$i $j"] = (i * pipelineUtil.subpacketSize + j) % pipelineUtil.numTotalUnits
                blockIndexMap["$i $j"] = (i * pipelineUtil.subpacketSize + j)
            }
        }
    }

    override fun fetchBlock(message: String) {
        val split = message.split(" ")
        val requesterNodeId = split[0].toInt()
        val blockId = split[1]
        val erasedIndex = split[2].toInt()
        val fetchMethod = split[3]
        if (fetchMethod == "pipeline") {
            fetchBlockUsingPipelining(requesterNodeId, blockId, erasedIndex)
        } else {
            fetchBlock(requesterNodeId, blockId, erasedIndex)
        }
    }

    fun fetchBlock(requesterNodeId: Int, blockId: String, erasedIndex: Int) {
        val erasedIndexes = intArrayOf(erasedIndex)
        val util = ClayCodeErasureDecodingStep.ClayCodeUtil(
            erasedIndexes, pipelineUtil.numDataUnits, pipelineUtil.numParityUnits
        )
        val parityIndexes = (pipelineUtil.numDataUnits until pipelineUtil.numTotalUnits).toList().toIntArray()
        val clayCode = ClayCode(pipelineUtil.numDataUnits, pipelineUtil.numParityUnits, pipelineUtil.clayBlockSize, parityIndexes)
        val originalInputs = clayCode.getInputs()
        val originalOutputs = clayCode.getOutputs()
        val encodedResult = clayCode.encode(originalInputs, originalOutputs)
        val constructInputs = clayCode.getTestInputs(encodedResult[0], encodedResult[1], erasedIndexes)

        val inputs = Array((pipelineUtil.numDataUnits + pipelineUtil.numParityUnits) * pipelineUtil.subpacketSize) {
            ECBlock(false, false)
        }
        val prevTime = System.nanoTime()

        for (i in 0 until pipelineUtil.subpacketSize) {
            for (j in 0 until pipelineUtil.numDataUnits + pipelineUtil.numParityUnits) {
                val inputIndex = i * (pipelineUtil.numDataUnits + pipelineUtil.numParityUnits) + j

                if (j != erasedIndex) {
                    val fileName = "$blockId.$j.$i"
                    val message = "$fileName $COORDINATOR_IP $PORT_NUMBER"
                    jedis.publish("$HELPER_CHANNEL_PREFIX.$j.send.data", message)
                    val data = receiveData(pipelineUtil.clayBlockSize)[0]
                    inputs[inputIndex] = ECBlock(ECChunk(ByteBuffer.wrap(data)), false, false)
                }
            }
        }

        logger.info("Time taken to fetch inputs: ${(System.nanoTime() - prevTime)/1000000000.0}")

        val outputsArray = Array(pipelineUtil.subpacketSize) { Array(erasedIndexes.size) { ByteBuffer.wrap(ByteArray(pipelineUtil.clayBlockSize)) } }

        val clayCodeHelper = ClayCodeHelper(pipelineUtil.numDataUnits, pipelineUtil.numParityUnits, pipelineUtil.subpacketSize, inputs)
        clayCodeHelper.getHelperPlanesAndDecode(util, blockId, outputsArray, erasedIndex, pipelineUtil.clayBlockSize, false)

        val currentTime = System.nanoTime()
        val timeElapsed = (currentTime - prevTime)/1000000000.0
        logger.info("Completed fetch block: $blockId without using pipelining")
        logger.info("Time taken: $timeElapsed")
        cleanup()
    }

    override fun fetchBlockUsingPipelining(finalNodeId: Int, blockId: String, erasedIndex: Int) {
        fetchBlockUsingPipelining(finalNodeId, erasedIndex, blockId)
    }

    fun fetchBlockUsingPipelining(finalNodeId: Int, erasedIndex: Int, blockId: String) {
        val erasedIndexes = intArrayOf(erasedIndex)
        val isDirect = true
        val util = ClayCodeErasureDecodingStep.ClayCodeUtil(
            erasedIndexes, pipelineUtil.numDataUnits, pipelineUtil.numParityUnits
        )
        val parityIndexes = (pipelineUtil.numDataUnits until pipelineUtil.numTotalUnits).toList().toIntArray()
        val clayCode = ClayCode(pipelineUtil.numDataUnits, pipelineUtil.numParityUnits, pipelineUtil.clayBlockSize, parityIndexes)
        val originalInputs = clayCode.getInputs()
        val originalOutputs = clayCode.getOutputs()
        val encodedResult = clayCode.encode(originalInputs, originalOutputs)

        terminatedMap.clear()
        for ((host, port) in nodeHostMap) {
            jedis.xtrim("$host $port", 0, false)
        }

        val inputs = clayCode.getTestInputs(encodedResult[0], encodedResult[1], erasedIndexes)

        for (i in 0 until pipelineUtil.numTotalUnits) {
            val futures = terminatedMap[blockId] ?: mutableMapOf()
            futures[i] = CompletableFuture()
            terminatedMap[blockId] = futures
        }

        prevTime = System.nanoTime()
        logger.info("Starting decode")

        decode(util, blockId, erasedIndex)
    }

    fun cleanup() {
        val files = File(".").listFiles { dir, name ->
            name.startsWith("LP ") || name.startsWith("ORIGINAL")
        } ?: emptyArray()
        for (file in files) {
            file.delete()
        }
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
            cleanup()
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
            "$blockId $subpacketIndex ${pipelineUtil.clayBlockSize}"
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
        logger.debug("Send and store decoupled data to $receiverNode from $senderNode")
        waitForJedis(receiverHost, receiverPort)
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$receiverNode.receive.decoupled.data",
            "$blockId $receiverSubpacketIndex ${pipelineUtil.clayBlockSize}"
        )
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$senderNode.send.data.for.decouple",
            "$blockId $senderSubpacketIndex ${pipelineUtil.clayBlockSize} $receiverHost $receiverPort"
        )
        jedis.xadd("$receiverHost $receiverPort", null, mapOf("lock" to "$senderNode"))
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
            waitForJedis(receiverHost, receiverPort)
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$receiverId.receive.output.data",
                "$blockId $subpacketIndex ${pipelineUtil.clayBlockSize} ${erasedDecoupledNodes.size}"
            )
            val first = i == 0
            val message = "$blockId $subpacketIndex ${pipelineUtil.clayBlockSize}" +
                    " $receiverHost $receiverPort $i" +
                    " ${pipelineUtil.numDataUnits} ${pipelineUtil.numParityUnits}" +
                    " ${erasedDecoupledNodes.joinToString(",")} $first"
            jedis.publish("$HELPER_CHANNEL_PREFIX.$senderId.decode.and.send", message)
            jedis.xadd("$receiverHost $receiverPort", null, mapOf("lock" to "$senderId"))
        }

        // Send to erased nodes
        val lastNodeId = nodesPath.last()
        val receiverHosts = mutableListOf<String>()
        val receiverPorts = mutableListOf<Int>()
        val outputIndexes = mutableListOf<Int>()
        for (index in erasedDecoupledNodes.indices) {
            val receiverId = erasedDecoupledNodes[index]
            val nodeHostPort = nodeHostMap[receiverId] ?: throw Exception("Host not found for $receiverId")
            val receiverHost = nodeHostPort.first
            val receiverPort = nodeHostPort.second
            receiverHosts.add(receiverHost)
            receiverPorts.add(receiverPort)
            waitForJedis(receiverHost, receiverPort)
            outputIndexes.add(index)
            jedis.publish(
                "$HELPER_CHANNEL_PREFIX.$receiverId.receive.decoded.data",
                "$blockId $subpacketIndex ${pipelineUtil.clayBlockSize}"
            )
            jedis.xadd("$receiverHost $receiverPort", null, mapOf("lock" to "$lastNodeId"))
        }
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$lastNodeId.send.decoded.data",
            "$blockId $subpacketIndex ${pipelineUtil.clayBlockSize}" +
                    " ${receiverHosts.joinToString(",")} ${receiverPorts.joinToString(",")}" +
                    " ${nodesPath.lastIndex} ${outputIndexes.joinToString(",")}" +
                    " ${pipelineUtil.numDataUnits} ${pipelineUtil.numParityUnits}" +
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
            "$blockId $subpacketIndex ${pipelineUtil.clayBlockSize}"
        )
    }

    private fun sendErasedData(
        blockId: String, nodeIndex: Int, coupleSubpacketIndex: Int, decoupleSubpacketIndex: Int,
        zIndex: Int,
        receiverId: Int, receiverHost: String, receiverPort: Int
    ) {
        logger.debug("Node $nodeIndex sending erased data to $receiverId")
        waitForJedis(receiverHost, receiverPort)
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$receiverId.receive.erased.data",
            "$blockId $zIndex ${pipelineUtil.clayBlockSize}"
        )
        jedis.publish(
            "$HELPER_CHANNEL_PREFIX.$nodeIndex.send.erased.data",
            "$blockId $coupleSubpacketIndex $decoupleSubpacketIndex ${pipelineUtil.clayBlockSize} $receiverHost $receiverPort"
        )
        jedis.xadd("$receiverHost $receiverPort", null, mapOf("lock" to "$nodeIndex"))
    }

    private fun getNodesPath(blockId: String, erasedNodes: IntArray): List<Int> {
        return (0 until pipelineUtil.numTotalUnits).filterNot { erasedNodes.contains(it) }
    }

    @Synchronized
    private fun receiveData(blockSize: Int, noOfBlocks: Int = 1): Array<ByteArray> {
        return serverSocket.accept().use { socket ->
            DataInputStream(BufferedInputStream(socket.getInputStream())).use { socketIn ->
                // Read 8KB blocks
                var bytesRead = 0
                val buffer = ByteBuffer.allocate(blockSize * noOfBlocks)
                while (socketIn.available() < 0) {
                    // Wait for data
                }
                while (bytesRead < blockSize * noOfBlocks) {
                    val data = socketIn.readBytes()
                    buffer.put(data)
                    bytesRead += data.size
                }
                buffer.flip()
                val result = Array(noOfBlocks) { ByteArray(blockSize) }
                for (i in 0 until noOfBlocks) {
                    buffer.get(result[i])
                }
                result
            }
        }
    }

    private fun waitForJedis(receiverHost: String, receiverPort: Int) {
        logger.debug("Wait for data: $receiverHost $receiverPort")
        var stream = jedis.xread(Integer.MAX_VALUE, 10, java.util.AbstractMap.SimpleImmutableEntry(
            "$receiverHost $receiverPort", StreamEntryID()
        ))
        if (stream.isEmpty() || stream.none { it.key == "$receiverHost $receiverPort" }) {
            return
        }
        var lastEntry = stream.last { it.key == "$receiverHost $receiverPort" }
        var lastStreamEntryId = lastEntry.value.last().id
        while (lastEntry.value.last().fields["lock"] != "released") {
            stream = jedis.xread(Integer.MAX_VALUE, 10, java.util.AbstractMap.SimpleImmutableEntry(
                "$receiverHost $receiverPort", lastStreamEntryId
            ))
            if (stream.isNotEmpty()) {
                lastEntry = stream.last { it.key == "$receiverHost $receiverPort" }
                lastStreamEntryId = lastEntry.value.last().id
            }
        }
    }
}

fun main() {
    val nodeHostMap = (0 until 16)
        .map { Pair(it, Pair("127.0.0.1", 1111*(it+1))) }
        .toMap()

    val blockNodeMap = LinkedHashMap<String, Int>()
    val blockIndexMap = mutableMapOf<String, Int>()

    val coordinator = ClayCoordinator(nodeHostMap.toMutableMap(), blockNodeMap, blockIndexMap)

    while (true) {
        val latch = CountDownLatch(1)
        coordinator.latch = latch
        latch.await()
    }
}