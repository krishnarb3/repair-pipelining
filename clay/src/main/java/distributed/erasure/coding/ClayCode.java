package distributed.erasure.coding;

import com.backblaze.erasure.ReedSolomon;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClayCode {

    public static final int NUM_DATA_UNITS = 4;
    public static final int NUM_PARITY_UNITS = 2;
    public static final int BLOCK_SIZE = 8704;
    private static boolean startBufferWithZero = true;

    private int[] erasedIndexes;
    private ClayCodeUtil util;

    public void performCoding(ByteBuffer[] inputs, ByteBuffer[] outputs)
            throws Exception {

        final int numDataUnits = NUM_DATA_UNITS;
        final int numParityUnits = NUM_PARITY_UNITS;
        final int numTotalUnits = numDataUnits + numParityUnits;
        final int subPacketSize = util.getSubPacketSize();

        ByteBuffer firstValidInput = ClayCodeUtil.findFirstValidInput(inputs);
        final int bufSize = firstValidInput.remaining();
        final boolean isDirect = firstValidInput.isDirect();

        if (inputs.length != numTotalUnits * util.getSubPacketSize()) {
            throw new IllegalArgumentException("Invalid inputs length");
        }

        if (outputs.length != erasedIndexes.length * util.getSubPacketSize()) {
            throw new IllegalArgumentException("Invalid outputs length");
        }

        List<Integer> erasedIndexesList = Arrays.stream(erasedIndexes)
                .boxed().collect(Collectors.<Integer>toList());

        // inputs length = numDataUnits * subPacketizationSize
        ByteBuffer[][] newIn = new ByteBuffer[subPacketSize][numTotalUnits];
        for (int i = 0; i < subPacketSize; ++i) {
            for (int j = 0; j < numTotalUnits; ++j) {
                if (!erasedIndexesList.contains(j)) {
                    newIn[i][j] = inputs[i * numTotalUnits + j];
                }
            }
        }

        ByteBuffer[][] newOut = new ByteBuffer[subPacketSize][erasedIndexes.length];
        for (int i = 0; i < subPacketSize; ++i) {
            for (int j = 0; j < erasedIndexes.length; ++j) {
                newOut[i][j] = outputs[i * erasedIndexes.length + j];
            }
        }

        doDecodeMulti(newIn, newOut, bufSize, isDirect);

        int erasedIndex = 1;

        ByteBuffer[][] testIn = new ByteBuffer[subPacketSize][numTotalUnits];
        for (int i = 0; i < subPacketSize; ++i) {
            for (int j = 0; j < numTotalUnits; ++j) {
                if (j != erasedIndex) {
                    if (j < numDataUnits) {
                        testIn[i][j] = inputs[i * numTotalUnits + j];
                    } else {
                        testIn[i][j] = newOut[i][j - numDataUnits];
                    }
                }
            }
        }

        ByteBuffer[][] testOut = new ByteBuffer[subPacketSize][1];
        for (int i = 0; i < subPacketSize; i++) {
            for (int j = 0; j < 1; j++) {
                testOut[i][j] = ByteBuffer.wrap(new byte[bufSize]);
            }
        }

//        if (erasedIndexes.length == 1) {
//            doDecodeSingle(newIn, newOut, erasedIndexes[0], bufSize, isDirect);
//        } else {
//            doDecodeMulti(newIn, newOut, bufSize, isDirect);
//        }

        doDecodeSingle(testIn, testOut, erasedIndex, bufSize, isDirect);

        for (int i = 0; i < subPacketSize; i++) {
            for (int j = 0; j < 1; j++) {
                assert testOut[i][j] == newOut[i][1];
            }
        }
    }


    /**
     * Decode a single erased nodes in the given inputs. The algorithm uses the corresponding helper planes to reconstruct
     * the erased node
     *
     * @param inputs      numDataUnits * subPacket sized byte buffers
     * @param outputs     numErasedIndexes * subPacket sized byte buffers
     * @param erasedIndex the index of the erased node
     * @throws IOException
     */
    private void doDecodeSingle(ByteBuffer[][] inputs,
                                ByteBuffer[][] outputs,
                                int erasedIndex,
                                int bufSize,
                                boolean isDirect)
            throws Exception {

        int[][] inputPositions = new int[inputs.length][inputs[0].length];
        for (int i = 0; i < inputs.length; ++i) {
            for (int j = 0; j < inputs[i].length; ++j) {
                if (inputs[i][j] != null) {
                    inputPositions[i][j] = inputs[i][j].position();
                }
            }
        }

        int[][] outputPositions = new int[outputs.length][outputs[0].length];
        for (int i = 0; i < outputs.length; i++) {
            for (int j = 0; j < outputs[i].length; j++) {
                if (outputs[i][j] != null) {
                    outputPositions[i][j] = outputs[i][j].position();
                }
            }
        }

    /*
    get the indices of all the helper planes
    helper planes are the ones with hole dot pairs
    */
        int[] helperIndexes = util.getHelperPlanesIndexes(erasedIndex);
        ByteBuffer[][] helperCoupledPlanes = new ByteBuffer[helperIndexes.length][inputs[0].length];

        getHelperPlanes(inputs, helperCoupledPlanes, erasedIndex);


        ByteBuffer[] tmpOutputs = new ByteBuffer[2];

        for (int p = 0; p < 2; ++p)
            tmpOutputs[p] = ClayCodeUtil.allocateByteBuffer(isDirect, bufSize);


        int y = util.getNodeCoordinates(erasedIndex)[1];
        int[] erasedDecoupledNodes = new int[util.q];

    /*
    the couples can not be found for any of the nodes in the same column as the erased node
    erasedDecoupledNodes is a list of all those nodes
    */
        for (int x = 0; x < util.q ; x++) {
            erasedDecoupledNodes[x] = util.getNodeIndex(x,y);
        }

        for (int i=0; i<helperIndexes.length; ++i){

            int z = helperIndexes[i];
            ByteBuffer[] helperDecoupledPlane = new ByteBuffer[inputs[0].length];

            getDecoupledHelperPlane(helperCoupledPlanes, helperDecoupledPlane, i, helperIndexes, erasedIndex, bufSize, isDirect);
            decodeDecoupledPlane(helperDecoupledPlane, erasedDecoupledNodes, bufSize, isDirect);

            //after getting all the values in decoupled plane, find out q erased values
            for (int x = 0; x <util.q; x++) {
                int nodeIndex = util.getNodeIndex(x, y);

                if (nodeIndex == erasedIndex) {
                    outputs[z][0].put(helperDecoupledPlane[nodeIndex]);
                } else {

                    int coupledZIndex = util.getCouplePlaneIndex(new int[]{x, y}, z);

                    getPairWiseCouple(new ByteBuffer[]{null, helperCoupledPlanes[i][nodeIndex], null, helperDecoupledPlane[nodeIndex]}, tmpOutputs);

                    outputs[coupledZIndex][0].put(tmpOutputs[0]);

                    // clear the temp- buffers for reuse
                    for (int p = 0; p < 2; ++p)
                        tmpOutputs[p].clear();

                }

            }
        }

        //restoring the positions of output buffers back
        for (int i = 0; i < outputs.length; i++) {
            for (int j = 0; j < outputs[i].length; j++) {
                outputs[i][j].position(outputPositions[i][j]);
            }
        }

        //restoring the positions of input buffers back
//        for (int i = 0; i < inputs.length; i++) {
//            for (int j = 0; j < inputs[i].length; j++) {
//                if (inputs[i][j] != null) {
//                    inputs[i][j].position(inputPositions[i][j] +bufSize);
//                }
//            }
//        }
    }

    private void doDecodeMulti(ByteBuffer[][] inputs, ByteBuffer[][] outputs,
                               int bufSize, boolean isDirect) throws IOException {
        int[][] inputPositions = new int[inputs.length][inputs[0].length];
        for (int i = 0; i < inputs.length; ++i) {
            for (int j = 0; j < inputs[i].length; ++j) {
                if (inputs[i][j] != null) {
                    inputPositions[i][j] = inputs[i][j].position();
                }
            }
        }

        int[][] outputPositions = new int[outputs.length][outputs[0].length];
        for (int i = 0; i < outputs.length; i++) {
            for (int j = 0; j < outputs[i].length; j++) {
                if (outputs[i][j] != null) {
                    outputPositions[i][j] = outputs[i][j].position();
                }
            }
        }

        Map<Integer, ArrayList<Integer>> ISMap = util.getAllIntersectionScores();
        int maxIS = Collections.max(ISMap.keySet());

        // is is the current intersection score
        for (int is = 0; is <= maxIS; ++is) {

            ArrayList<Integer> realZIndexes = ISMap.get(is);
            if (realZIndexes == null) continue;

            // Given the IS, for each zIndex in ISMap.get(i), temp stores the corresponding
            // decoupled plane in the same order as in the array list.
            ByteBuffer[][] temp = new ByteBuffer[realZIndexes.size()][NUM_DATA_UNITS + NUM_PARITY_UNITS];
            int idx = 0;

            for (int z : realZIndexes) {
                getDecoupledPlane(inputs, temp[idx], z, bufSize, isDirect);
                decodeDecoupledPlane(temp[idx], erasedIndexes, bufSize, isDirect);
                idx++;

            }


            ByteBuffer[] tmpOutputs = new ByteBuffer[2];

            for (int p = 0; p < 2; ++p)
                tmpOutputs[p] = ClayCodeUtil.allocateByteBuffer(isDirect, bufSize);


            for (int j = 0; j < temp.length; ++j) {
                for (int k = 0; k < erasedIndexes.length; ++k) {

                    // Find the erasure type ans correspondingly decode
                    int erasedIndex = erasedIndexes[k];

                    int erasureType = util.getErasureType(erasedIndex, realZIndexes.get(j));

                    if (erasureType == 0) {
                        inputs[realZIndexes.get(j)][erasedIndex] = temp[j][erasedIndex];
                        outputs[realZIndexes.get(j)][k].put(temp[j][erasedIndex]);

                    } else {

                        // determine the couple plane and coordinates
                        int[] z_vector = util.getZVector(realZIndexes.get(j));
                        int[] coordinates = util.getNodeCoordinates(erasedIndex);
                        int couplePlaneIndex = util.getCouplePlaneIndex(coordinates, realZIndexes.get(j));
                        int coupleIndex = util.getNodeIndex(z_vector[coordinates[1]], coordinates[1]);

                        if (erasureType == 1) {
                            getPairWiseCouple(new ByteBuffer[]{null, inputs[couplePlaneIndex][coupleIndex], temp[j][erasedIndex], null}, tmpOutputs);

                            inputs[realZIndexes.get(j)][erasedIndex] = ClayCodeUtil.cloneBufferData(tmpOutputs[0]);
                            outputs[realZIndexes.get(j)][k].put(tmpOutputs[0]);


                        } else {
                            // determine the corresponding index of the couple plane in the temp buffers
                            int tempCoupleIndex = realZIndexes.indexOf(couplePlaneIndex);

                            getPairWiseCouple(new ByteBuffer[]{null, null, temp[j][erasedIndex], temp[tempCoupleIndex][coupleIndex]}, tmpOutputs);

                            inputs[realZIndexes.get(j)][erasedIndex] = ClayCodeUtil.cloneBufferData(tmpOutputs[0]);
                            outputs[realZIndexes.get(j)][k].put(tmpOutputs[0]);

                        }

                        // clear the temp- buffers for reuse
                        for (int p = 0; p < 2; ++p)
                            tmpOutputs[p].clear();
                    }
                }
            }
        }

        //restoring the positions of output buffers back
        for (int i = 0; i < outputs.length; i++) {
            for (int j = 0; j < outputs[i].length; j++) {
                outputs[i][j].position(outputPositions[i][j]);
            }
        }

        //restoring the positions of input buffers back
        for (int i = 0; i < inputs.length; i++) {
            for (int j = 0; j < inputs[i].length; j++) {
                inputs[i][j].position(inputPositions[i][j] + bufSize);
            }
        }
    }

    /**
     * Find the fill the helper planes corresponding to the erasedIndex from the inputs
     *
     * @param inputs       numDataUnits * subPacket sized byte buffers
     * @param helperPlanes numHelperPlanes * subPacket byte buffers
     * @param erasedIndex  the erased index
     */
    private void getHelperPlanes(ByteBuffer[][] inputs, ByteBuffer[][] helperPlanes, int erasedIndex) {
        int[] helperIndexes = util.getHelperPlanesIndexes(erasedIndex);

        for (int i = 0; i < helperIndexes.length; ++i) {
            helperPlanes[i] = inputs[helperIndexes[i]];
        }

    }

    /**
     * Convert all the input symbols of the given plane into its decoupled form. We use the pairWiseRawDecoder to
     * achieve this.
     *
     * @param helperPlanes     an array of all the helper planes
     * @param temp             array to write the decoded plane to
     * @param helperPlaneIndex the index of the plane to be decoded
     * @param helperIndexes    list of all the helper planes
     * @param erasedIndex      the erased node index
     * @param bufSize          default buffer size
     * @throws IOException
     */
    private void getDecoupledHelperPlane(ByteBuffer[][] helperPlanes, ByteBuffer[] temp, int helperPlaneIndex,
                                         int[] helperIndexes, int erasedIndex,
                                         int bufSize, boolean isDirect)
            throws IOException {

        int z = helperIndexes[helperPlaneIndex];
        int[] z_vec = util.getZVector(z);

        ByteBuffer[] tmpOutputs = new ByteBuffer[2];

        for (int idx = 0; idx < 2; ++idx)
            tmpOutputs[idx] = ClayCodeUtil.allocateByteBuffer(isDirect, bufSize);

        int[] erasedCoordinates = util.getNodeCoordinates(erasedIndex);

        for (int i = 0; i < util.q * util.t; i++) {

            int[] coordinates = util.getNodeCoordinates(i);

            if (coordinates[1] != erasedCoordinates[1]) {

                if (z_vec[coordinates[1]] == coordinates[0]) {
                    temp[i] = helperPlanes[helperPlaneIndex][i];
                } else {

                    int coupleZIndex = util.getCouplePlaneIndex(coordinates, z);
                    int coupleHelperPlaneIndex = 0;

                    for (int j = 0; j < helperIndexes.length; j++) {
                        if (helperIndexes[j] == coupleZIndex) {
                            coupleHelperPlaneIndex = j;
                            break;
                        }
                    }

                    int coupleCoordinates = util.getNodeIndex(z_vec[coordinates[1]], coordinates[1]);

                    getPairWiseCouple(new ByteBuffer[]{helperPlanes[helperPlaneIndex][i],
                                    helperPlanes[coupleHelperPlaneIndex][coupleCoordinates], null, null},
                            tmpOutputs);

                    temp[i] = ClayCodeUtil.cloneBufferData(tmpOutputs[0]);

                    // clear the buffers for reuse
                    for (int idx = 0; idx < 2; ++idx)
                        tmpOutputs[idx].clear();

                }
            }

        }

    }

    /**
     * Convert all the input symbols of the given plane into its decoupled form. We use the rsRawDecoder to achieve this.
     *
     * @param z    plane index
     * @param temp temporary array which stores decoupled values
     */
    private void getDecoupledPlane(ByteBuffer[][] inputs, ByteBuffer[] temp, int z, int bufSize, boolean isDirect)
            throws IOException {
        int[] z_vec = util.getZVector(z);


        ByteBuffer[] tmpOutputs = new ByteBuffer[2];

        for (int idx = 0; idx < 2; ++idx)
            tmpOutputs[idx] = ClayCodeUtil.allocateByteBuffer(isDirect, bufSize);

        for (int i = 0; i < util.q * util.t; i++) {
            int[] coordinates = util.getNodeCoordinates(i);

            if (inputs[z][i] != null) {
                // If the coordinates correspond to a dot in the plane
                if (z_vec[coordinates[1]] == coordinates[0])
                    temp[i] = inputs[z][i];
                else {

                    int coupleZIndex = util.getCouplePlaneIndex(coordinates, z);
                    int coupleCoordinates = util.getNodeIndex(z_vec[coordinates[1]], coordinates[1]);

                    getPairWiseCouple(new ByteBuffer[]{inputs[z][i], inputs[coupleZIndex][coupleCoordinates], null, null}, tmpOutputs);

                    temp[i] = ClayCodeUtil.cloneBufferData(tmpOutputs[0]);

                    // clear the buffers for reuse
                    for (int idx = 0; idx < 2; ++idx)
                        tmpOutputs[idx].clear();
                }
            } else {
                temp[i] = null;
            }
        }
    }

    /**
     * Decode and complete the decoupled plane
     *
     * @param decoupledPlane the plane to be decoded
     * @throws IOException
     */
    private void decodeDecoupledPlane(ByteBuffer[] decoupledPlane, int[] erasedIndexes, int bufSize, boolean isDirect)
            throws IOException {


        ByteBuffer[] tmpOutputs = new ByteBuffer[erasedIndexes.length];

        for (int idx = 0; idx < erasedIndexes.length; ++idx)
            tmpOutputs[idx] = ClayCodeUtil.allocateByteBuffer(isDirect, bufSize);

        int[] inputPos = new int[decoupledPlane.length];

        int r = 0;
        for (int i = 0; i < decoupledPlane.length; ++i) {
            if (decoupledPlane[i] != null)
                inputPos[i] = decoupledPlane[i].position();
            else {
                inputPos[i] = tmpOutputs[r++].position();
            }
        }

        ReedSolomon rs = ReedSolomon.create(NUM_DATA_UNITS, NUM_PARITY_UNITS);
        byte[][] shards = new byte[decoupledPlane.length][];
        boolean[] shardsPresent = new boolean[decoupledPlane.length];
        for (int i = 0; i < decoupledPlane.length; i++) {
            ByteBuffer buff = decoupledPlane[i];
            if (buff != null) {
                shards[i] = buff.array();
            } else {
                shards[i] = new byte[BLOCK_SIZE];
            }
            int currentIndex = i;
            shardsPresent[i] = Arrays.stream(erasedIndexes).noneMatch(index -> index == currentIndex);
        }

        // Replace with backblaze RS implementation
        rs.decodeMissing(shards, shardsPresent, 0, bufSize);
//        rsRawDecoder.decode(decoupledPlane, erasedIndexes, tmpOutputs);

        int outputCounter = 0;
        for (int i = 0; i < shards.length; i++) {
            if (!shardsPresent[i]) {
                tmpOutputs[outputCounter] = ByteBuffer.wrap(shards[i]);
                outputCounter++;
            }
        }

        for (int i = 0; i < erasedIndexes.length; ++i) {
            decoupledPlane[erasedIndexes[i]] = tmpOutputs[i];
        }

        for (int i = 0; i < decoupledPlane.length; ++i) {
            decoupledPlane[i].position(inputPos[i]);
        }

    }


    /**
     * Get the pairwise couples of the given inputs. Use the pairWiseDecoder to do this.
     * Notationally inputs are (A,A',B,B') and the outputs array contain the unknown values of the inputs
     *
     * @param inputs  pairwise known couples
     * @param outputs pairwise couples of the known values
     */
    private void getPairWiseCouple(ByteBuffer[] inputs, ByteBuffer[] outputs)
            throws IOException {
        int[] lostCouples = new int[2];
        int[] inputPos = new int[inputs.length];
        int[] outputPos = new int[outputs.length];

        for (int i = 0; i < outputs.length; ++i) {
            outputPos[i] = outputs[i].position();
        }

        int k = 0;

        for (int i = 0; i < inputs.length; ++i) {
            if (inputs[i] == null) {
                lostCouples[k++] = i;
            } else {
                inputPos[i] = inputs[i].position();
            }
        }

        PairwiseRSDecoder pairWiseDecoder = new PairwiseRSDecoder(2, 2);

        List<ByteBuffer> actualInputs = Arrays.stream(inputs).filter(Objects::nonNull).collect(Collectors.toList());
        byte[][] inputByteArray = new byte[inputs.length][];
        for (int i = 0; i < inputs.length; i++) {
            if (i < actualInputs.size()) {
                inputByteArray[i] = actualInputs.get(i).array();
            } else {
                inputByteArray[i] = new byte[actualInputs.get(0).array().length];
            }
        }
        Integer[] lostCouplesArray = new Integer[lostCouples.length];
        for (int i = 0; i < lostCouples.length; i++) {
            lostCouplesArray[i] = lostCouples[i];
        }

        pairWiseDecoder.decode(inputByteArray, lostCouplesArray, BLOCK_SIZE);

        for (int i = 0; i < inputs.length; ++i) {
            if (inputs[i] != null) {
                inputs[i].position(inputPos[i]);
            }
        }

        for (int i = 2; i < 4; i++) {
            outputs[i - 2] = ByteBuffer.wrap(inputByteArray[i]);
        }

        for (int i = 0; i < outputs.length; ++i) {
            outputs[i].position(outputPos[i]);
        }

    }

    public int[] getErasedIndexes() {
        return erasedIndexes;
    }

    public void setErasedIndexes(int[] erasedIndexes) {
        this.erasedIndexes = erasedIndexes;
    }

    public ClayCodeUtil getUtil() {
        return util;
    }

    public void setUtil(ClayCodeUtil util) {
        this.util = util;
    }

    public static void main(String[] args) throws Exception {
        int[] erasedIndexes = new int[] { 4, 5 };
        ClayCodeUtil clayCodeUtil = new ClayCodeUtil(erasedIndexes, NUM_DATA_UNITS, NUM_PARITY_UNITS);
        ClayCode clayCode = new ClayCode();
        clayCode.util = clayCodeUtil;
        clayCode.setErasedIndexes(erasedIndexes);

        int dataLength = (NUM_DATA_UNITS) * clayCodeUtil.getSubPacketSize();
        int inputsLength = (NUM_DATA_UNITS + NUM_PARITY_UNITS) * clayCodeUtil.getSubPacketSize();
        int outputsLength = (erasedIndexes.length)  * clayCodeUtil.getSubPacketSize();

        File inputFile = new File("LP-block.jpg");
        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(inputFile));

        ByteBuffer[] inputs = new ByteBuffer[(NUM_DATA_UNITS + NUM_PARITY_UNITS) * clayCodeUtil.getSubPacketSize()];

        int counter = 0;
        for (int i = 0; i < inputsLength; i++) {
            if (counter < NUM_DATA_UNITS) {
                inputs[i] = allocateOutputBuffer(BLOCK_SIZE, dataInputStream.readNBytes(BLOCK_SIZE));
            }
            counter++;
            counter = counter % (NUM_DATA_UNITS + NUM_PARITY_UNITS);
        }

        ByteBuffer[] outputs = new ByteBuffer[erasedIndexes.length * clayCodeUtil.getSubPacketSize()];
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = ByteBuffer.allocate(BLOCK_SIZE);
        }


        clayCode.performCoding(inputs, outputs);

//        ByteBuffer[] newInputs = new ByteBuffer[inputsLength];
//        // Erase 2,3 indices
//        int total = NU0M_DATA_UNITS + NUM_PARITY_UNITS;
//        int outputCounter = 0;
//        for (int i = 0; i < newInputs.length; i++) {
//            if ((i+1) % total == 2 || (i+1) % total == 3) {
//                newInputs[i] = ByteBuffer.wrap(new byte[BLOCK_SIZE]);
//            } else {
//                if ((i+1) % total == 5 || (i+1) % total == 0) {
//                    newInputs[i] = ByteBuffer.wrap(outputs[outputCounter].array());
//                    newInputs[i].position(outputs[outputCounter].position());
//                    newInputs[i].limit(outputs[outputCounter].limit());
//                    outputCounter++;
//                } else {
//                    newInputs[i] = ByteBuffer.wrap(inputs[i].array());
//                    newInputs[i].position(inputs[i].position());
//                    newInputs[i].limit(inputs[i].limit());
//                }
//            }
//        }
//        clayCode.setErasedIndexes(new int[] {2, 3});
//
//        ByteBuffer[] newOutputs = new ByteBuffer[erasedIndexes.length * clayCodeUtil.getSubPacketSize()];
//        for (int i = 0; i < outputs.length; i++) {
//            newOutputs[i] = ByteBuffer.allocate(BLOCK_SIZE);
//        }
//
//        clayCode.performCoding(newInputs, newOutputs);

        dataInputStream.close();
    }

    protected static ByteBuffer allocateOutputBuffer(int bufferLen, byte[] data) {
        /**
         * When startBufferWithZero, will prepare a buffer as:---------------
         * otherwise, the buffer will be like:             ___TO--BE--WRITTEN___,
         * and in the beginning, dummy data are prefixed, to simulate a buffer of
         * position > 0.
         */
        int startOffset = startBufferWithZero ? 0 : 11; // 11 is arbitrary
        int allocLen = startOffset + bufferLen + startOffset;
        ByteBuffer buffer = ByteBuffer.allocate(allocLen);
        buffer.limit(startOffset + bufferLen);
        buffer.put(data);
        buffer.flip();
        startBufferWithZero = ! startBufferWithZero;

        return buffer;
    }
}
