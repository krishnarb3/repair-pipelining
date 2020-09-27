/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package distributed.erasure.coding.clay;

import com.backblaze.erasure.ReedSolomon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class ClayCodeErasureDecodingStep {

    private int[] erasedIndexes;
    private ReedSolomon pairWiseDecoder;
    private ReedSolomon rsRawDecoder;

    private ClayCodeUtil util;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * Basic constructor with necessary info
     *
     * @param pairWiseDecoder Decoder for the pair wise transforms
     * @param rsRawDecoder    Decoder for each layer
     */
    public ClayCodeErasureDecodingStep(int[] erasedIndexes,
                                       ReedSolomon pairWiseDecoder,
                                       ReedSolomon rsRawDecoder) {
        this.erasedIndexes = erasedIndexes;
        this.rsRawDecoder = rsRawDecoder;
        this.pairWiseDecoder = pairWiseDecoder;

        this.util = new ClayCodeUtil(erasedIndexes, rsRawDecoder.getDataShardCount(), rsRawDecoder.getParityShardCount());
    }

    public void performCoding(ECChunk[] inputChunks, ECChunk[] outputChunks) throws IOException {
        if (erasedIndexes.length == 0) {
            return;
        }

        ByteBuffer[] inputBuffers = ECChunk.toBuffers(inputChunks);
        ByteBuffer[] outputBuffers = ECChunk.toBuffers(outputChunks);
        performCoding(inputBuffers, outputBuffers);

    }

    private void performCoding(ByteBuffer[] inputs, ByteBuffer[] outputs)
            throws IOException {

        final int numDataUnits = rsRawDecoder.getDataShardCount();
        final int numParityUnits = rsRawDecoder.getParityShardCount();
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

        // inputs length = numDataUnits * subPacketizationSize
        ByteBuffer[][] newIn = new ByteBuffer[subPacketSize][numTotalUnits];
        for (int i = 0; i < subPacketSize; ++i) {
            for (int j = 0; j < numTotalUnits; ++j) {
                newIn[i][j] = inputs[i * numTotalUnits + j];
            }
        }

        ByteBuffer[][] newOut = new ByteBuffer[subPacketSize][erasedIndexes.length];
        for (int i = 0; i < subPacketSize; ++i) {
            for (int j = 0; j < erasedIndexes.length; ++j) {
                newOut[i][j] = outputs[i * erasedIndexes.length + j];
            }
        }

        if (erasedIndexes.length==1) {
            doDecodeSingle(newIn, newOut, erasedIndexes[0], bufSize, isDirect);
        } else {
            doDecodeMulti(newIn, newOut, bufSize, isDirect);
        }

        logger.debug("Completed coding step");

    }


    /**
     * Decode a single erased nodes in the given inputs. The algorithm uses the corresponding helper planes to reconstruct
     * the erased node
     * @param inputs  numDataUnits * subPacket sized byte buffers
     * @param outputs numErasedIndexes * subPacket sized byte buffers
     * @param erasedIndex the index of the erased node
     * @throws IOException
     */
    public void doDecodeSingle(ByteBuffer[][] inputs,
                                ByteBuffer[][] outputs,
                                int erasedIndex,
                                int bufSize,
                                boolean isDirect)
            throws IOException{

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
        logger.debug("Helper indexes: " + Arrays.toString(helperIndexes));
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
            decodeDecoupledPlane(helperDecoupledPlane, erasedDecoupledNodes, bufSize, isDirect, true);

            //after getting all the values in decoupled plane, find out q erased values
            for (int x = 0; x <util.q; x++) {
                int nodeIndex = util.getNodeIndex(x, y);

                if (nodeIndex == erasedIndex) {
                    outputs[z][0].put(helperDecoupledPlane[nodeIndex]);
                } else {

                    int coupledZIndex = util.getCouplePlaneIndex(new int[]{x, y}, z);

                    logger.trace("Helper plane [" + i + "][" + nodeIndex + "]");

                    getPairWiseCouple(new ByteBuffer[]{null, helperCoupledPlanes[i][nodeIndex], null, helperDecoupledPlane[nodeIndex]}, tmpOutputs, bufSize);

                    outputs[coupledZIndex][0].put(tmpOutputs[0]);

                    // clear the temp- buffers for reuse
                    for (int p = 0; p < 2; ++p)
                        tmpOutputs[p].clear();

                }
            }

            logger.trace("Z done: " + z);
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
                if (inputs[i][j] != null) {
                    inputs[i][j].position(inputPositions[i][j] +bufSize);
                }
            }
        }

    }

    // helperCoupledPlanes[][i]
    // helperCoupledPlanes[][coupledCoordinates]
    public void doDecodeSingle(
            ByteBuffer[][] helperCoupledPlanes,
            int[] helperIndexes,
            int i,
            ByteBuffer[][] outputs,
            int erasedIndex, int bufSize, boolean isDirect) throws IOException {
        /*
        get the indices of all the helper planes
        helper planes are the ones with hole dot pairs
        */

        ByteBuffer[] tmpOutputs = new ByteBuffer[2];

        for (int p = 0; p < 2; ++p)
            tmpOutputs[p] = ClayCodeUtil.allocateByteBuffer(isDirect, bufSize);


        int y = util.getNodeCoordinates(erasedIndex)[1];
        int[] erasedDecoupledNodes = new int[util.q];

        logger.trace("Erased decoupled nodes size: " + erasedDecoupledNodes.length);

    /*
    the couples can not be found for any of the nodes in the same column as the erased node
    erasedDecoupledNodes is a list of all those nodes
    */
        for (int x = 0; x < util.q ; x++) {
            erasedDecoupledNodes[x] = util.getNodeIndex(x,y);
        }

        int z = helperIndexes[i];
        ByteBuffer[] helperDecoupledPlane = new ByteBuffer[util.q * util.t];

        getDecoupledHelperPlane(helperCoupledPlanes, helperDecoupledPlane, i, helperIndexes, erasedIndex, bufSize, isDirect);
        decodeDecoupledPlane(helperDecoupledPlane, erasedDecoupledNodes, bufSize, isDirect, true);

        //after getting all the values in decoupled plane, find out q erased values
        for (int x = 0; x <util.q; x++) {
            int nodeIndex = util.getNodeIndex(x, y);

            if (nodeIndex == erasedIndex) {
                outputs[z][0].put(helperDecoupledPlane[nodeIndex]);
            } else {

                int coupledZIndex = util.getCouplePlaneIndex(new int[]{x, y}, z);

                getPairWiseCouple(new ByteBuffer[]{null, helperCoupledPlanes[i][nodeIndex], null, helperDecoupledPlane[nodeIndex]}, tmpOutputs, bufSize);

                outputs[coupledZIndex][0].put(tmpOutputs[0]);

                // clear the temp- buffers for reuse
                for (int p = 0; p < 2; ++p)
                    tmpOutputs[p].clear();

            }

        }
    }


    /**
     * Find the fill the helper planes corresponding to the erasedIndex from the inputs
     * @param inputs numDataUnits * subPacket sized byte buffers
     * @param helperPlanes numHelperPlanes * subPacket byte buffers
     * @param erasedIndex the erased index
     */
    private void getHelperPlanes(ByteBuffer[][] inputs, ByteBuffer[][] helperPlanes, int erasedIndex){
        int[] helperIndexes = util.getHelperPlanesIndexes(erasedIndex);

        for(int i=0; i<helperIndexes.length; ++i){
            helperPlanes[i] = inputs[helperIndexes[i]];
        }

        logger.debug("getHelperPlanes completed");

    }


    /**
     * Decode multiple erased nodes in the given inputs. The algorithm decodes the planes of different
     * intersections scores sequentially starting from IS=0 to its maximum possible value.
     *
     * @param inputs  numDataUnits * subPacket sized byte buffers
     * @param outputs numErasedIndexes * subPacket sized byte buffers
     * @throws IOException
     */
    private void doDecodeMulti(ByteBuffer[][] inputs, ByteBuffer[][] outputs,
                               int bufSize, boolean isDirect)
            throws IOException {

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
            ByteBuffer[][] temp = new ByteBuffer[realZIndexes.size()][rsRawDecoder.getDataShardCount() + rsRawDecoder.getParityShardCount()];
            int idx = 0;

            for (int z : realZIndexes) {
                getDecoupledPlane(inputs, temp[idx], z, bufSize, isDirect);
                decodeDecoupledPlane(temp[idx], erasedIndexes, bufSize, isDirect, false);
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
                            getPairWiseCouple(new ByteBuffer[]{null, inputs[couplePlaneIndex][coupleIndex], temp[j][erasedIndex], null}, tmpOutputs, bufSize);

                            inputs[realZIndexes.get(j)][erasedIndex] = ClayCodeUtil.cloneBufferData(tmpOutputs[0]);
                            outputs[realZIndexes.get(j)][k].put(tmpOutputs[0]);


                        } else {
                            // determine the corresponding index of the couple plane in the temp buffers
                            int tempCoupleIndex = realZIndexes.indexOf(couplePlaneIndex);

                            getPairWiseCouple(new ByteBuffer[]{null, null, temp[j][erasedIndex], temp[tempCoupleIndex][coupleIndex]}, tmpOutputs, bufSize);

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
     * Convert all the input symbols of the given plane into its decoupled form. We use the pairWiseRawDecoder to
     * achieve this.
     * @param helperPlanes an array of all the helper planes
     * @param temp array to write the decoded plane to
     * @param helperPlaneIndex the index of the plane to be decoded
     * @param helperIndexes list of all the helper planes
     * @param erasedIndex the erased node index
     * @param bufSize default buffer size
     * @throws IOException
     */
    private void getDecoupledHelperPlane(ByteBuffer[][] helperPlanes, ByteBuffer[] temp, int helperPlaneIndex,
                                         int[] helperIndexes, int erasedIndex,
                                         int bufSize, boolean isDirect )
            throws IOException {

        logger.debug("Get decoupled helper plane: " + helperPlaneIndex);

        int z = helperIndexes[helperPlaneIndex];
        int[] z_vec = util.getZVector(z);

        ByteBuffer[] tmpOutputs = new ByteBuffer[2];

        for (int idx = 0; idx < 2; ++idx)
            tmpOutputs[idx] = ClayCodeUtil.allocateByteBuffer(isDirect, bufSize);

        int[] erasedCoordinates = util.getNodeCoordinates(erasedIndex);

        for (int i = 0; i < util.q*util.t; i++) {

            int[] coordinates = util.getNodeCoordinates(i);

            if(coordinates[1]!=erasedCoordinates[1]){

                if (z_vec[coordinates[1]] == coordinates[0]){
                    logger.trace("Helper plane [" + helperPlaneIndex + "][" + i + "]");
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

                    logger.trace("Helper plane [" + coupleHelperPlaneIndex + "][" + coupleCoordinates + "]");

                    getPairWiseCouple(new ByteBuffer[]{helperPlanes[helperPlaneIndex][i],
                                    helperPlanes[coupleHelperPlaneIndex][coupleCoordinates], null, null},
                            tmpOutputs, bufSize);

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

                    getPairWiseCouple(new ByteBuffer[]{inputs[z][i], inputs[coupleZIndex][coupleCoordinates], null, null}, tmpOutputs, bufSize);

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
    private void decodeDecoupledPlane(
            ByteBuffer[] decoupledPlane, int[] erasedIndexes,
            int bufSize, boolean isDirect, boolean isSingle
    ) throws IOException {


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

        boolean[] shardPresent = getShardPresent(decoupledPlane, erasedIndexes);
        byte[][] decoupledPlaneAsBytes = getByteArray(decoupledPlane, bufSize);

        logger.debug("erasedIndexes: " + Arrays.toString(erasedIndexes));

        byte[][] outputs = new byte[erasedIndexes.length][bufSize];

        if (isSingle && System.getProperty("isTest") != null && System.getProperty("isTest").equals("true")) {
            for (int i = 0; i < decoupledPlaneAsBytes.length - erasedIndexes.length; i++) {
                if (i == 0) {
                    rsRawDecoder.decodeMissingSingle(decoupledPlaneAsBytes[i + erasedIndexes.length], i + erasedIndexes.length, i, shardPresent, outputs, 0, bufSize, true);
                } else {
                    rsRawDecoder.decodeMissingSingle(decoupledPlaneAsBytes[i + erasedIndexes.length], i + erasedIndexes.length, i, shardPresent, outputs, 0, bufSize, false);
                }
            }
            for (int i = 0; i < outputs.length; i++) {
                decoupledPlaneAsBytes[erasedIndexes[i]] = outputs[i];
            }
        } else if (isSingle) {
            rsRawDecoder.decodeMissing(decoupledPlaneAsBytes, shardPresent, 0, bufSize);
        } else {
            rsRawDecoder.decodeMissing(decoupledPlaneAsBytes, shardPresent, 0, bufSize);
        }

        for (int i = 0; i < erasedIndexes.length; ++i) {
            ByteBuffer buffer = ByteBuffer.wrap(decoupledPlaneAsBytes[erasedIndexes[i]]);
            decoupledPlane[erasedIndexes[i]] = buffer;
        }

        for (int i = 0; i < decoupledPlane.length; ++i) {
            decoupledPlane[i].position(inputPos[i]);
        }

    }

    private byte[][] getByteArray(ByteBuffer[] decoupledPlane, int bufSize) {
        byte[][] result = new byte[decoupledPlane.length][];
        for (int i = 0; i < decoupledPlane.length; i++) {
            if (decoupledPlane[i] != null) {
                result[i] = decoupledPlane[i].array();
            } else {
                result[i] = new byte[bufSize];
            }
        }
        return result;
    }

    private boolean[] getShardPresent(ByteBuffer[] decoupledPlane, int[] erasedIndexes) {
        boolean[] shardPresent = new boolean[decoupledPlane.length];
        Arrays.fill(shardPresent, true);
        for (int erasedIndex : erasedIndexes) {
            if (erasedIndex < decoupledPlane.length) {
                shardPresent[erasedIndex] = false;
            }
        }
        return shardPresent;
    }


    /**
     * Get the pairwise couples of the given inputs. Use the pairWiseDecoder to do this.
     * Notationally inputs are (A,A',B,B') and the outputs array contain the unknown values of the inputs
     *
     * @param inputs  pairwise known couples
     * @param outputs pairwise couples of the known values
     */
    public void getPairWiseCouple(ByteBuffer[] inputs, ByteBuffer[] outputs, int bufSize)
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

        byte[][] inputsAsBytes = getByteArray(inputs, bufSize);
        boolean[] shardPresent = getShardPresent(inputs, lostCouples);

        logger.trace("Pairwise decoder: inputSize: " + inputs.length);
        pairWiseDecoder.decodeMissing(inputsAsBytes, shardPresent, 0, bufSize);

        for (int i = 0; i < inputs.length; ++i) {
            inputs[i] = ByteBuffer.wrap(inputsAsBytes[i]);
            inputs[i].position(inputPos[i]);
        }

        for (int i = 0; i < outputs.length; ++i) {
            outputs[i] = ByteBuffer.wrap(inputsAsBytes[lostCouples[i]]);
            outputs[i].position(outputPos[i]);
        }

    }


    public void finish() {
        // TODO
    }

    /**
     * Basic utilities for ClayCode encode/decode and repair operations.
     */
    public static class ClayCodeUtil {
        public int q, t;
        private int subPacketSize;
        private int[] erasedIndexes;


        /**
         * Clay codes are parametrized via two parameters q and t such that numOfAllUnits = q*t and numOfParityUnits = q.
         * The constructor initialises these
         *
         * @param erasedIndexes  indexes of the erased nodes
         * @param numDataUnits
         * @param numParityUnits
         */
        public ClayCodeUtil(int[] erasedIndexes, int numDataUnits, int numParityUnits) {
            this.q = numParityUnits;
            this.t = (numParityUnits + numDataUnits) / numParityUnits;
            this.erasedIndexes = erasedIndexes;
            this.subPacketSize = (int) Math.pow(q, t);
        }

        public static ByteBuffer allocateByteBuffer(boolean useDirectBuffer,
                                                    int bufSize) {
            if (useDirectBuffer) {
                return ByteBuffer.allocateDirect(bufSize);
            } else {
                return ByteBuffer.allocate(bufSize);
            }
        }

        /**
         * Find the valid input from all the inputs.
         *
         * @param inputs input buffers to look for valid input
         * @return the first valid input
         */
        public static <T> T findFirstValidInput(T[] inputs) {
            for (T input : inputs) {
                if (input != null) {
                    return input;
                }
            }

            throw new IllegalArgumentException(
                    "Invalid inputs are found, all being null");
        }

        public static ByteBuffer cloneBufferData(ByteBuffer srcBuffer) {
            ByteBuffer destBuffer;
            byte[] bytesArr = new byte[srcBuffer.remaining()];

            srcBuffer.mark();
            srcBuffer.get(bytesArr);
            srcBuffer.reset();

            if (!srcBuffer.isDirect()) {
                destBuffer = ByteBuffer.wrap(bytesArr);
            } else {
                destBuffer = ByteBuffer.allocateDirect(srcBuffer.remaining());
                destBuffer.put(bytesArr);
                destBuffer.flip();
            }

            return destBuffer;
        }

        /**
         * Get the subPacketizationSize
         * @return SubPacket size
         */
        public int getSubPacketSize() {
            return subPacketSize;
        }

        /**
         * Get the index of the plane as an integer from a base q notation.
         * eg. for q=4, t=5 ; 25 = [0,0,1,2,1]
         *
         * @param z_vector plane index in vector form
         * @return z plane index in integer form
         */
        public int getZ(int[] z_vector) {
            int z = 0;
            int power = 1;

            for (int i = this.t - 1; i >= 0; --i) {
                z += z_vector[i] * power;
                power *= this.q;
            }
            return z;
        }

        /**
         * Get the base q notation of the plane
         *
         * @param z plane index in integer form
         * @return plane index in vector form
         */
        public int[] getZVector(int z) {
            int[] z_vec = new int[this.t];

            for (int i = this.t - 1; i >= 0; --i) {
                z_vec[i] = z % this.q;
                z /= this.q;
            }

            return z_vec;
        }

        /**
         * Intersection score of a plane is the number of hole-dot pairs in that plane.
         * <p>
         * ------- * --
         * |   |   |   |
         * --- *-- O---
         * |   |   |   |
         * + ----------
         * |   |   |   |
         * O --------- *
         * </p>
         * + indicates both a dot and hole pair, * denotes a dot and a erasure(hole) O.
         * The above has q = 4 and t = 4.
         * So intersection of the above plane = 1.
         *
         * @param z_vector plane in vector form
         * @return intersection score of the plane
         */

        public int getIntersectionScore(int[] z_vector) {
            int intersectionScore = 0;
            for (int i = 0; i < this.erasedIndexes.length; ++i) {
                int index = this.erasedIndexes[i];
                int[] a = getNodeCoordinates(index);
                int x = a[0];
                int y = a[1];

                if (z_vector[y] == x) {
                    intersectionScore = intersectionScore + 1;
                }
            }
            return intersectionScore;
        }

        /**
         * For each intersection score finds out indices of all the planes whose intersection score = i.
         *
         * @return map of intersection scores and the corresponding z indexes
         */
        public Map<Integer, ArrayList<Integer>> getAllIntersectionScores() {
            Map<Integer, ArrayList<Integer>> hm = new HashMap<>();
            for (int i = 0; i < (int) Math.pow(q, t); i = i + 1) {
                int[] z_vector = getZVector(i);
                int intersectionScore = getIntersectionScore(z_vector);

                if (!hm.containsKey(intersectionScore)) {
                    ArrayList<Integer> arraylist = new ArrayList<>();
                    arraylist.add(i);
                    hm.put(intersectionScore, arraylist);
                } else {
                    hm.get(intersectionScore).add(i);
                }
            }
            return hm;
        }

        /**
         * @param x x coordinate of the node in plane
         * @param y y coordinate of the node in plane
         * @return x+y*q
         */

        public int getNodeIndex(int x, int y) {
            return x + q * y;
        }

        /**
         * @param index index of the node in integer
         * @return (x, y) coordinates of the node
         */
        public int[] getNodeCoordinates(int index) {
            int[] a = new int[2];
            a[0] = index % q;
            a[1] = index / q;
            return a;
        }

        /**
         * The following are the erasure types
         * <p>
         * ------- * --
         * |   |   |   |
         * --- *-- O --
         * |   |   |   |
         * + ----------
         * |   |   |   |
         * O --------- *
         * </p>
         * + indicates both a dot * and a erasure(hole) O
         * The above has q = 4 and t = 4.
         * (2,0) is an erasure of type 0
         * (3,0) is an erasure of type 2
         * (1,2) is an erasure of type 1
         *
         * @param indexInPlane integer index of the erased node in the plane (x+y*q)
         * @param z            index of the plane
         * @return return the error type for a node in a plane.
         * Erasure types possible are : {0,1,2}
         */
        public int getErasureType(int indexInPlane, int z) {

            int[] z_vector = getZVector(z);
            int[] nodeCoordinates = getNodeCoordinates(indexInPlane);

            // there is a hole-dot pair at the given index => type 0
            if (z_vector[nodeCoordinates[1]] == nodeCoordinates[0])
                return 0;

            int dotInColumn = getNodeIndex(z_vector[nodeCoordinates[1]], nodeCoordinates[1]);

            // there is a hole dot pair in the same column => type 2
            for (int i = 0; i < this.erasedIndexes.length; ++i) {
                if (this.erasedIndexes[i] == dotInColumn)
                    return 2;
            }
            // there is not hole dot pair in the same column => type 1
            return 1;

        }

        /**
         * Get the index of the couple plane of the given coordinates
         *
         * @param coordinates
         * @param z
         */
        public int getCouplePlaneIndex(int[] coordinates, int z) {
            int[] coupleZvec = getZVector(z);
            coupleZvec[coordinates[1]] = coordinates[0];
            return getZ(coupleZvec);
        }


        /**
         * Get the helper planes indexes associated with a failure k
         *
         * @param k erased node index.
         * @return all the planes which have a hole-dot pair at k.
         */
        public int[] getHelperPlanesIndexes(int k) {

            int[] a = getNodeCoordinates(k);
            int x = a[0];
            int y = a[1];

            int exp = (int) Math.pow(q, t - 1);
            int zIndexes[] = new int[exp];

            int j = 0;
            for (int i = 0; i < ((int) Math.pow(q, t)); i++) {
                int[] zVector = getZVector(i);
                if (zVector[y] == x)
                    zIndexes[j++] = i;
            }

            return zIndexes;
        }


    }
}
