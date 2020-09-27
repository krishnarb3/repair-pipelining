package distributed.erasure.coding.clay;

import com.backblaze.erasure.ReedSolomon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class ClayCode {
    private static boolean startBufferWithZero = true;

    private int numDataUnits;
    private int numParityUnits;
    private int blockSize;
    private int[] erasedIndexes;
    private ReedSolomon pairwiseDecoder;
    private ReedSolomon rsRawDecoder;
    private ClayCodeErasureDecodingStep.ClayCodeUtil clayCodeUtil;
    private ClayCodeErasureDecodingStep erasureDecodingStep;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public ClayCode(int numDataUnits, int numParityUnits, int blockSize, int[] erasedIndexes) {
        this.numDataUnits = numDataUnits;
        this.numParityUnits = numParityUnits;
        this.blockSize = blockSize;
        this.erasedIndexes = erasedIndexes;
        this.pairwiseDecoder = ReedSolomon.create(2, 2);
        this.rsRawDecoder = ReedSolomon.create(numDataUnits, numParityUnits);
        this.clayCodeUtil = new ClayCodeErasureDecodingStep.ClayCodeUtil(
            erasedIndexes, numDataUnits, numParityUnits
        );
        this.erasureDecodingStep = new ClayCodeErasureDecodingStep(
            erasedIndexes, pairwiseDecoder, rsRawDecoder
        );
    }

    public void performCoding(ECChunk[] inputs, ECChunk[] outputs) throws Exception {
        erasureDecodingStep.performCoding(inputs, outputs);
    }

    public ECBlock[] getInputs() throws Exception {
        File inputFile = new File("LP-block.jpg");
        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(inputFile));
        Random random = new Random(123456);
        int inputsLength = (numDataUnits + numParityUnits) * clayCodeUtil.getSubPacketSize();

        ECBlock[] inputs = new ECBlock[inputsLength];

        int counter = 0;

        for (int i = 0; i < numDataUnits + numParityUnits; i++) {
            for (int j = 0; j < clayCodeUtil.getSubPacketSize(); j++) {
                int k = i * clayCodeUtil.getSubPacketSize() + j;
                if (counter < numDataUnits) {
                    byte[] array = new byte[blockSize];
                    random.nextBytes(array);
                    ByteBuffer buffer = allocateOutputBuffer(blockSize, array);
                    ECChunk chunk = new ECChunk(buffer);
                    inputs[k] = new ECBlock(chunk, false, false);
                } else {
                    ByteBuffer buffer = null;
                    ECChunk chunk = new ECChunk(buffer);
                    inputs[k] = new ECBlock(chunk, true, true);
                }
                counter++;
                counter = counter % (numDataUnits + numParityUnits);
            }
        }

        return inputs;
    }

    public ECBlock[] getOutputs() throws Exception {
        ECBlock[] outputs = new ECBlock[erasedIndexes.length * clayCodeUtil.getSubPacketSize()];
        for (int i = 0; i < outputs.length; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(blockSize);
            ECChunk chunk = new ECChunk(buffer);
            outputs[i] = new ECBlock(chunk, true, true);
        }
        return outputs;
    }

    public List<ECChunk[]> encode(ECBlock[] inputs, ECBlock[] outputs) throws Exception {
        ClayCodeErasureDecodingStep clayCodeErasureDecodingStep = new ClayCodeErasureDecodingStep(
                erasedIndexes, pairwiseDecoder, rsRawDecoder
        );

        ECChunk[] inputChunks = getChunks(inputs);
        ECChunk[] outputChunks = getChunks(outputs);
        clayCodeErasureDecodingStep.performCoding(inputChunks, outputChunks);

        return List.of(inputChunks, outputChunks);
    }

    public ECBlock[] getTestInputs(ECChunk[] inputChunks, ECChunk[] outputChunks, int[] testErasedIndexesArray) throws Exception {
        // Test
        List<Integer> testErasedIndexes = Arrays.stream(testErasedIndexesArray).boxed().collect(Collectors.toList());
        ECBlock[] testInputs = getTestInputs(inputChunks, outputChunks, testErasedIndexes, blockSize);
        return testInputs;
    }

    private ECBlock[] getTestInputs(ECChunk[] inputs, ECChunk[] outputs, List<Integer> erasedIndexes, int bufSize) {
        ECBlock[] testInputs = new ECBlock[inputs.length];
        int k = 0;

        // TODO: Change this
        String blockId = "LP";

        for (int a = 0; a < numDataUnits + numParityUnits; a++) {
            for (int b = 0; b < clayCodeUtil.getSubPacketSize(); b++) {
                int i = a * clayCodeUtil.getSubPacketSize() + b;
                if (!erasedIndexes.contains(a)) {
                    ByteBuffer buffer;
                    if (inputs[i].getBuffer() != null) {
                        buffer = allocateOutputBuffer(blockSize, inputs[i].getBuffer().array());
                    } else {
                        buffer = allocateOutputBuffer(blockSize, outputs[k].getBuffer().array());
                        k += 1;
                    }
                    ECChunk chunk = new ECChunk(buffer);
                    testInputs[i] = new ECBlock(chunk, false, true);
                } else {
                    if (inputs[i].getBuffer() == null) {
                        k++;
                    }
                    ByteBuffer buffer = ByteBuffer.allocate(bufSize);
                    ECChunk chunk = new ECChunk(buffer);
                    testInputs[i] = new ECBlock(chunk, false, true);
                }
            }
        }

        k = 0;
        for (int i = 0; i < clayCodeUtil.getSubPacketSize(); i++) {
            for (int j = 0; j < numDataUnits + numParityUnits; j++) {
                int inputIndex = i * (numDataUnits + numParityUnits) + j;
                String prefix = "";
                if (erasedIndexes.contains(j)) {
                    prefix = "ORIGINAL ";
                }
                if (inputs[inputIndex].getBuffer() != null) {
                    writeToFile(prefix + blockId + " " + j + " " + i, inputs[inputIndex].getBuffer().array());
                } else {
                    writeToFile(prefix + blockId + " " + j + " " + i, outputs[k++].getBuffer().array());
                }
            }
        }
        return testInputs;
    }

    public ECBlock[] getTestOutputs(int erasedIndexesSize) {
        int length = erasedIndexesSize * clayCodeUtil.getSubPacketSize();
        ECBlock[] testOutputs = new ECBlock[length];
        for (int i = 0; i < length; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(blockSize);
            ECChunk chunk = new ECChunk(buffer);
            testOutputs[i] = new ECBlock(chunk, false, true);
        }
        return testOutputs;
    }

    public ECChunk[] getChunks(ECBlock[] blocks) {
        ECChunk[] result = new ECChunk[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            ECBlock block = blocks[i];
            if (block != null) {
                result[i] = block.getChunk();
            }
        }
        return result;
    }

    private ByteBuffer allocateOutputBuffer(int bufferLen, byte[] data) {
        /*
         * When startBufferWithZero, will prepare a buffer as:---------------
         * otherwise, the buffer will be like:             ___TO--BE--WRITTEN___,
         * and in the beginning, dummy data are prefixed, to simulate a buffer of
         * position > 0.
         */
        int startOffset = startBufferWithZero ? 0 : 0; // 11 is arbitrary
        int allocLen = startOffset + bufferLen + startOffset;
        ByteBuffer buffer = ByteBuffer.allocate(allocLen);
        buffer.limit(startOffset + bufferLen);
        buffer.put(data);
        buffer.flip();
        startBufferWithZero = ! startBufferWithZero;

        return buffer;
    }

    private void writeToFile(String fileName, byte[] data) {
        File file = null;
        OutputStream outputStream = null;
        DataOutputStream dataOutputStream = null;
        try {
            file = new File(fileName);
            outputStream = new FileOutputStream(file);
            dataOutputStream = new DataOutputStream(outputStream);
            dataOutputStream.write(data);
            dataOutputStream.close();
            outputStream.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public int getNumDataUnits() {
        return numDataUnits;
    }

    public void setNumDataUnits(int numDataUnits) {
        this.numDataUnits = numDataUnits;
    }

    public int getNumParityUnits() {
        return numParityUnits;
    }

    public void setNumParityUnits(int numParityUnits) {
        this.numParityUnits = numParityUnits;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public ReedSolomon getPairwiseDecoder() {
        return pairwiseDecoder;
    }

    public void setPairwiseDecoder(ReedSolomon pairwiseDecoder) {
        this.pairwiseDecoder = pairwiseDecoder;
    }

    public ReedSolomon getRsRawDecoder() {
        return rsRawDecoder;
    }

    public void setRsRawDecoder(ReedSolomon rsRawDecoder) {
        this.rsRawDecoder = rsRawDecoder;
    }

    public ClayCodeErasureDecodingStep getErasureDecodingStep() {
        return erasureDecodingStep;
    }

    public void setErasureDecodingStep(ClayCodeErasureDecodingStep erasureDecodingStep) {
        this.erasureDecodingStep = erasureDecodingStep;
    }

    public void setErasedIndexes(int[] erasedIndexes) {
        this.erasedIndexes = erasedIndexes;
        this.clayCodeUtil = new ClayCodeErasureDecodingStep.ClayCodeUtil(erasedIndexes, numDataUnits, numParityUnits);
        this.erasureDecodingStep = new ClayCodeErasureDecodingStep(
                erasedIndexes, pairwiseDecoder, rsRawDecoder
        );
    }
}
