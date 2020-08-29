package distributed.erasure.coding.clay;

import java.util.List;

public class ClayCodeRunner {
    public static void main(String[] args) throws Exception {
        int numDataUnits = Integer.parseInt(args[0]);
        int numParityUnits = Integer.parseInt(args[1]);
        int blockSize = Integer.parseInt(args[2]);
        String[] split = args[3].split(",");
        int[] erasedIndexes = new int[split.length];
        for (int i = 0; i < split.length; i++) {
            erasedIndexes[i] = Integer.parseInt(split[i]);
        }

        int[] parityIndexes = new int[numParityUnits];
        for (int i = 0; i < parityIndexes.length; i++) {
            parityIndexes[i] = numDataUnits + i;
        }
        ClayCode clayCode = new ClayCode(numDataUnits, numParityUnits, blockSize, parityIndexes);

        ECBlock[] inputs = clayCode.getInputs();
        ECBlock[] outputs = clayCode.getOutputs();
        List<ECChunk[]> encodedResult = clayCode.encode(inputs, outputs);

        ECChunk[] encodedInputChunks = encodedResult.get(0);
        ECChunk[] encodedOutputChunks = encodedResult.get(1);

        clayCode = new ClayCode(numDataUnits, numParityUnits, blockSize, erasedIndexes);

        ECBlock[] testInputs = clayCode.getTestInputs(encodedInputChunks, encodedOutputChunks, erasedIndexes);
        ECBlock[] testOutputs = clayCode.getTestOutputs(erasedIndexes.length);

        ECChunk[] testInputChunks = clayCode.getChunks(testInputs);
        ECChunk[] testOutputChunks = clayCode.getChunks(testOutputs);
        clayCode.performCoding(testInputChunks, testOutputChunks);

        System.out.println("Checking");
    }
}
