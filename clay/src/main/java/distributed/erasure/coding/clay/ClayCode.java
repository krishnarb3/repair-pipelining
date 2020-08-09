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
    private static boolean startBufferWithZero = true;

    public static void main(String[] args) throws Exception {
        int[] erasedIndexes = new int[] { 4, 5 };
        ClayCodeErasureDecodingStep.ClayCodeUtil clayCodeUtil = new ClayCodeErasureDecodingStep.ClayCodeUtil(
                erasedIndexes, NUM_DATA_UNITS, NUM_PARITY_UNITS
        );

        int dataLength = (NUM_DATA_UNITS) * clayCodeUtil.getSubPacketSize();
        int inputsLength = (NUM_DATA_UNITS + NUM_PARITY_UNITS) * clayCodeUtil.getSubPacketSize();
        int outputsLength = (erasedIndexes.length)  * clayCodeUtil.getSubPacketSize();


        File inputFile = new File("LP-block.jpg");
        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(inputFile));

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

        ECBlock[] outputs = new ECBlock[erasedIndexes.length * clayCodeUtil.getSubPacketSize()];
        for (int i = 0; i < outputs.length; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
            ECChunk chunk = new ECChunk(buffer);
            outputs[i] = new ECBlock(chunk, true, true);
        }

        ReedSolomon pairwiseDecoder = ReedSolomon.create(2, 2);
        ReedSolomon rsRawDecoder = ReedSolomon.create(NUM_DATA_UNITS, NUM_PARITY_UNITS);

        ClayCodeErasureDecodingStep clayCodeErasureDecodingStep = new ClayCodeErasureDecodingStep(
            inputs, erasedIndexes, outputs, pairwiseDecoder, rsRawDecoder
        );

        ECChunk[] inputChunks = getChunks(inputs);
        ECChunk[] outputChunks = getChunks(outputs);
        clayCodeErasureDecodingStep.performCoding(inputChunks, outputChunks);

        // Test

        int[] testErasedIndexesArray = new int[] { 1, 2 };
        List<Integer> testErasedIndexes = Arrays.stream(testErasedIndexesArray).boxed().collect(Collectors.toList());
        ECBlock[] testInputs = getTestInputs(inputChunks, outputChunks, testErasedIndexes, BLOCK_SIZE);
        ECBlock[] testOutputs = getTestOutputs(outputs, BLOCK_SIZE);

        ClayCodeErasureDecodingStep testClayCodeErasureDecodingStep = new ClayCodeErasureDecodingStep(
                testInputs, testErasedIndexesArray, testOutputs, pairwiseDecoder, rsRawDecoder
        );

        ECChunk[] testInputChunks = getChunks(testInputs);
        ECChunk[] testOutputChunks = getChunks(testOutputs);
        testClayCodeErasureDecodingStep.performCoding(testInputChunks, testOutputChunks);

        int k = 0;
        for (Integer i : testErasedIndexes) {
            ECChunk expectedChunk = inputChunks[i];
            ECChunk actualChunk = testOutputChunks[k++];
            System.out.println("Checking");
        }

    }

    private static ECChunk[] getChunks(ECBlock[] blocks) {
        ECChunk[] result = new ECChunk[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            ECBlock block = blocks[i];
            if (block != null) {
                result[i] = block.getChunk();
            }
        }
        return result;
    }

    private static ECBlock[] getTestInputs(ECChunk[] inputs, ECChunk[] outputs, List<Integer> erasedIndexes, int bufSize) {
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

    private static ECBlock[] getTestOutputs(ECBlock[] outputs, int bufSize) {
        ECBlock[] testInputs = new ECBlock[outputs.length];
        for (int i = 0; i < outputs.length; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(bufSize);
            ECChunk chunk = new ECChunk(buffer);
            testInputs[i] = new ECBlock(chunk, false, true);
        }
        return testInputs;
    }

    protected static ByteBuffer allocateOutputBuffer(int bufferLen, byte[] data) {
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
