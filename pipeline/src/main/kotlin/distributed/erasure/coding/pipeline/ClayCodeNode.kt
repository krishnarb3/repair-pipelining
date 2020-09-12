package distributed.erasure.coding.pipeline

import com.backblaze.erasure.InputOutputByteTableCodingLoop
import com.backblaze.erasure.ReedSolomon
import distributed.erasure.coding.pipeline.Util.HELPER_CHANNEL_PREFIX
import mu.KotlinLogging
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPubSub
import java.io.*
import java.net.ServerSocket
import java.net.Socket
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch

class ClayCodeNode : Node {
    val dataMap = ConcurrentHashMap<String, Array<ByteArray>>()
    val decoupledDataMap = ConcurrentHashMap<String, ByteArray>()

    private val logger = KotlinLogging.logger {}

    private val LOCAL_IP = System.getProperty("node.local.ip")
    private val JEDIS_POOL_MAX_SIZE = System.getProperty("jedis.pool.max.size").toInt()
    private val nodeId = System.getProperty("node.local.id")
    private val PORT_NUMBER = System.getProperty("node.local.port").toInt()
    private val serverSocket = ServerSocket(PORT_NUMBER)

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

        logger.info("Node $nodeId initialized")
    }

    override fun fetchBlock(blockId: String, blockSize: Int, subPacketIndex: Int) {
        val data = ByteArray(blockSize)
        val inputFile = File(blockId)
        val fileData = DataInputStream(FileInputStream(inputFile))
        fileData.read(data, blockSize * subPacketIndex, blockSize)
        fileData.close()
    }

    private fun getJedisPubSub() = object : JedisPubSub() {
        override fun onPMessage(pattern: String, channel: String, message: String) {
            _latch.countDown()
            when (channel) {
                "$HELPER_CHANNEL_PREFIX.$nodeId.store.decoupled.data" -> storeDecoupledData(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.receive.decoupled.data" -> receiveDecoupledData(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.send.data.for.decouple" -> sendDataForDecouple(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.receive.output.data" -> receiveOutputData(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.decode.and.send" -> decodeAndSend(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.receive.decoded.data" -> receiveDecodedData(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.send.decoded.data" -> sendDecodedData(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.store.pairwise.couple" -> storePairwiseCouple(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.store.erased.data" -> storeErasedData(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.receive.erased.data" -> receiveErasedData(message)
                "$HELPER_CHANNEL_PREFIX.$nodeId.send.erased.data" -> sendErasedData(message)
                else -> {
                    logger.error("Received message in $channel - Doing nothing")
                }
            }
        }
    }

    @Synchronized
    private fun storeDecoupledData(message: String) {
        val (blockId, subpacketIndexString, blockSizeString) = message.split(" ")
        val subpacketIndex = subpacketIndexString.toInt()
        val blockSize = blockSizeString.toInt()
        val inputFile = File("$blockId $nodeId $subpacketIndex")
        val data = ByteArray(blockSize)
        DataInputStream(FileInputStream(inputFile)).use {
            it.read(data, 0, blockSize)
        }
        logger.info("Storing decoupled data: ($blockId $nodeId $subpacketIndex)")
        decoupledDataMap["$blockId $nodeId $subpacketIndex"] = data
    }

    @Synchronized
    private fun receiveDecoupledData(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()

        val data = receiveData(blockSize)

        val array1 = File("$blockId $nodeId $subpacketIndex").readBytes()
        val array2 = data[0]
        logger.info("Array1 size: ${array1.size}, Array2 size: ${array2.size}")
        val pairwiseDecoder = ReedSolomon(2, 2, InputOutputByteTableCodingLoop())
        val inputs = arrayOf(array1, array2, ByteArray(blockSize), ByteArray(blockSize))
        pairwiseDecoder.decodeMissing(
            inputs,
            booleanArrayOf(true, true, false, false),
            0,
            blockSize
        )

        logger.info("Received and storing decoupled data: ($blockId $nodeId $subpacketIndex)")
        decoupledDataMap["$blockId $nodeId $subpacketIndex"] = inputs[2] ?: throw Exception("Decoupled data is null")
    }

    @Synchronized
    private fun sendDataForDecouple(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()
        val host = split[3]
        val port = split[4].toInt()

        val file = File("$blockId $nodeId $subpacketIndex")
        val data = file.readBytes()
        sendData(blockSize, host, port, data)
    }

    @Synchronized
    private fun receiveOutputData(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()
        val outputSize = split[3].toInt()

        val data = receiveData(blockSize, outputSize)
        dataMap["$blockId $nodeId $subpacketIndex"] = data
    }

    @Synchronized
    private fun decodeAndSend(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()
        val receiverHost = split[3]
        val receiverPort = split[4].toInt()
        val index = split[5].toInt()
        val numDataUnits = split[6].toInt()
        val numParityUnits = split[7].toInt()
        val erasedIndexes = split[8].split(",").map { it.toInt() }

        val outputs = dataMap["$blockId $nodeId $subpacketIndex"]
            ?: Array(erasedIndexes.size) { ByteArray(blockSize) }
        val rsDecoder = ReedSolomon(numDataUnits, numParityUnits, InputOutputByteTableCodingLoop())
        val inputShard = decoupledDataMap["$blockId $nodeId $subpacketIndex"]
            ?: throw Exception("Decoupled data not found for ($blockId $nodeId $subpacketIndex)")
        val shardPresent = (0 until numDataUnits + numParityUnits).map { !erasedIndexes.contains(it) }.toBooleanArray()
        rsDecoder.decodeMissingSingle(inputShard, index, shardPresent, outputs, 0, blockSize)

        val data = ByteBuffer.allocate(erasedIndexes.size * blockSize)
        for (i in erasedIndexes) {
            data.put(outputs[i])
        }
        sendData(blockSize, receiverHost, receiverPort, data.array())
    }

    @Synchronized
    private fun receiveDecodedData(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()

        val data = receiveData(blockSize)
        logger.info("Received decoupled data: ($blockId $nodeId $subpacketIndex)")
        decoupledDataMap["$blockId $nodeId $subpacketIndex"] = data[0]
    }

    @Synchronized
    private fun sendDecodedData(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()
        val receiverHost = split[3]
        val receiverPort = split[4].toInt()
        val index = split[5].toInt()
        val outputIndex = split[6].toInt()
        val numDataUnits = split[7].toInt()
        val numParityUnits = split[8].toInt()
        val erasedIndexes = split[9].split(",").map { it.toInt() }.toTypedArray()

        val outputs = dataMap["$blockId $nodeId $subpacketIndex"]
            ?: throw Exception("Couldn't find output data for block: $blockId, plane: $subpacketIndex")
        val rsDecoder = ReedSolomon(numDataUnits, numParityUnits, InputOutputByteTableCodingLoop())
        val inputShard = decoupledDataMap["$blockId $nodeId $subpacketIndex"]
        val shardPresent = (0 until numDataUnits + numParityUnits).map { !erasedIndexes.contains(it) }.toBooleanArray()
        rsDecoder.decodeMissingSingle(inputShard, index, shardPresent, outputs, 0, blockSize)

        sendData(blockSize, receiverHost, receiverPort, outputs[outputIndex])
    }

    @Synchronized
    private fun storePairwiseCouple(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1]
        val blockSize = split[2]

        val decoupledData = decoupledDataMap["$blockId $nodeId $subpacketIndex"] ?: File("$blockId $nodeId $subpacketIndex").readBytes()
        logger.info("Storing decoupled data (pairwiseCouple): ($blockId $nodeId $subpacketIndex)")
        decoupledDataMap["$blockId $nodeId $subpacketIndex"] = decoupledData
    }

    @Synchronized
    private fun storeErasedData(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()

        val data = decoupledDataMap["$blockId $nodeId $subpacketIndex"]
            ?: throw Exception("Decoupled data not found for ($blockId $nodeId $subpacketIndex)")
        logger.info("Stored erased data in ($blockId $nodeId $subpacketIndex)")
        File("$blockId $nodeId $subpacketIndex").writeBytes(data)
    }

    @Synchronized
    private fun receiveErasedData(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()

        val data = receiveData(blockSize)
        logger.info("Stored erased data in ($blockId $nodeId $subpacketIndex)")
        File("$blockId $nodeId $subpacketIndex").writeBytes(data[0])
    }

    @Synchronized
    private fun sendErasedData(message: String) {
        val split = message.split(" ")
        val blockId = split[0]
        val subpacketIndex = split[1].toInt()
        val blockSize = split[2].toInt()
        val receiverHost = split[3]
        val receiverPort = split[4].toInt()

        val outputs = ByteArray(blockSize)
        val pairwiseDecoder = ReedSolomon(2, 2, InputOutputByteTableCodingLoop())
        val coupledData = File("$blockId $nodeId $subpacketIndex").readBytes()
        val inputs = arrayOf(ByteArray(blockSize), coupledData, ByteArray(blockSize), decoupledDataMap["$blockId $nodeId $subpacketIndex"])
        pairwiseDecoder.decodeMissing(inputs, booleanArrayOf(false, true, false, true), 0, blockSize)

        sendData(blockSize, receiverHost, receiverPort, inputs[1]!!)
    }

    override fun setLatch(latch: CountDownLatch) { this._latch = latch }

    @Synchronized
    private fun receiveData(blockSize: Int, noOfBlocks: Int = 1): Array<ByteArray> {
        serverSocket.accept().use { socket ->
            DataInputStream(BufferedInputStream(socket.getInputStream())).use { socketIn ->
                while (socketIn.available() <= 0) {
                    // Wait for data
                }
                return (0 until noOfBlocks).map {
                    socketIn.readNBytes(blockSize)
                }.toTypedArray()
            }
        }
    }

    @Synchronized
    private fun sendData(blockSize: Int, receiverHost: String, receiverPort: Int, data: ByteArray) {
        Socket(receiverHost, receiverPort).use { socket ->
            DataOutputStream(BufferedOutputStream(socket.getOutputStream())).use { socketOut ->
                socketOut.write(data)
            }
        }
    }
}

fun main() {
    val clayNode = ClayCodeNode()

    while (true) {
        val latch = CountDownLatch(1)
        clayNode.setLatch(latch)
        latch.await()
    }
}