package distributed.erasure.coding.clay;

import com.backblaze.erasure.ReedSolomon;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ClayCode {
    public static final int NUM_DATA_UNITS = 4;
    public static final int NUM_PARITY_UNITS = 2;
    public static final int BLOCK_SIZE = 8704;
    public static int[] erasedIndexes = new int[] { 4, 5 };
    private static boolean startBufferWithZero = true;

    public static ReedSolomon pairwiseDecoder = ReedSolomon.create(2, 2);
    public static ReedSolomon rsRawDecoder = ReedSolomon.create(NUM_DATA_UNITS, NUM_PARITY_UNITS);

    public static ClayCodeErasureDecodingStep.ClayCodeUtil clayCodeUtil = new ClayCodeErasureDecodingStep.ClayCodeUtil(
            erasedIndexes, NUM_DATA_UNITS, NUM_PARITY_UNITS
    );

    public static ECBlock[] getInputs() throws Exception {
        File inputFile = new File("LP-block.jpg");
        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(inputFile));

        int inputsLength = (NUM_DATA_UNITS + NUM_PARITY_UNITS) * clayCodeUtil.getSubPacketSize();

        ECBlock[] inputs = new ECBlock[(NUM_DATA_UNITS + NUM_PARITY_UNITS) * clayCodeUtil.getSubPacketSize()];

        int counter = 0;
        for (int i = 0; i < inputsLength; i++) {
            if (counter < NUM_DATA_UNITS) {
                ByteBuffer buffer = allocateOutputBuffer(BLOCK_SIZE, dataInputStream.readNBytes(BLOCK_SIZE));
                ECChunk chunk = new ECChunk(buffer);
                inputs[i] = new ECBlock(chunk, false, false);
            } else {
                ByteBuffer buffer = null;
                ECChunk chunk = new ECChunk(buffer);
                inputs[i] = new ECBlock(chunk, true, true);
            }
            counter++;
            counter = counter % (NUM_DATA_UNITS + NUM_PARITY_UNITS);
        }

        return inputs;
    }

    public static ECBlock[] getTestInputs(ECBlock[] inputs) throws Exception {
        ECBlock[] outputs = new ECBlock[erasedIndexes.length * clayCodeUtil.getSubPacketSize()];
        for (int i = 0; i < outputs.length; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
            ECChunk chunk = new ECChunk(buffer);
            outputs[i] = new ECBlock(chunk, true, true);
        }

        ClayCodeErasureDecodingStep clayCodeErasureDecodingStep = new ClayCodeErasureDecodingStep(
                erasedIndexes, pairwiseDecoder, rsRawDecoder
        );

        ECChunk[] inputChunks = getChunks(inputs);
        ECChunk[] outputChunks = getChunks(outputs);
        clayCodeErasureDecodingStep.performCoding(inputChunks, outputChunks);

        // Test

        int[] testErasedIndexesArray = new int[] { 1 };
        List<Integer> testErasedIndexes = Arrays.stream(testErasedIndexesArray).boxed().collect(Collectors.toList());
        ECBlock[] testInputs = getTestInputs(inputChunks, outputChunks, testErasedIndexes, BLOCK_SIZE);
        return testInputs;
    }

    public static void main(String[] args) throws Exception {
        int[] testErasedIndexesArray = new int[] { 1 };
        List<Integer> testErasedIndexes = Arrays.stream(testErasedIndexesArray).boxed().collect(Collectors.toList());
        ClayCodeErasureDecodingStep testClayCodeErasureDecodingStep = new ClayCodeErasureDecodingStep(
                testErasedIndexesArray, pairwiseDecoder, rsRawDecoder
        );

        ECBlock[] inputs = getInputs();
        ECBlock[] testInputs = getTestInputs(inputs);
        ECBlock[] testOutputs = getTestOutputs(testErasedIndexesArray.length * clayCodeUtil.getSubPacketSize(), BLOCK_SIZE);

        ECChunk[] testInputChunks = getChunks(testInputs);
        ECChunk[] testOutputChunks = getChunks(testOutputs);
        testClayCodeErasureDecodingStep.performCoding(testInputChunks, testOutputChunks);

        System.out.println("Checking");
    }

    public static ECChunk[] getChunks(ECBlock[] blocks) {
        ECChunk[] result = new ECChunk[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            ECBlock block = blocks[i];
            if (block != null) {
                result[i] = block.getChunk();
            }
        }
        return result;
    }

    public static ECBlock[] getTestInputs(ECChunk[] inputs, ECChunk[] outputs, List<Integer> erasedIndexes, int bufSize) {
        ECBlock[] testInputs = new ECBlock[inputs.length];
        int k = 0;
        for (int i = 0; i < inputs.length; i++) {
            if (!erasedIndexes.contains(i)) {
                ByteBuffer buffer;
                if (inputs[i].getBuffer() != null) {
                    buffer = allocateOutputBuffer(BLOCK_SIZE, inputs[i].getBuffer().array());
                } else {
                    buffer = allocateOutputBuffer(BLOCK_SIZE, outputs[k++].getBuffer().array());
                }
                ECChunk chunk = new ECChunk(buffer);
                testInputs[i] = new ECBlock(chunk, false, true);
            } else {
                ByteBuffer buffer = ByteBuffer.allocate(bufSize);
                ECChunk chunk = new ECChunk(buffer);
                testInputs[i] = new ECBlock(chunk, false, true);
            }
        }
        return testInputs;
    }

    public static ECBlock[] getTestOutputs(int length, int bufSize) {
        ECBlock[] testOutputs = new ECBlock[length];
        for (int i = 0; i < length; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(bufSize);
            ECChunk chunk = new ECChunk(buffer);
            testOutputs[i] = new ECBlock(chunk, false, true);
        }
        return testOutputs;
    }

    public static ByteBuffer allocateOutputBuffer(int bufferLen, byte[] data) {
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
}
