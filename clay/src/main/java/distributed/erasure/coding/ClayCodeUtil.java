package distributed.erasure.coding;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ClayCodeUtil {
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
    ClayCodeUtil(int[] erasedIndexes, int numDataUnits, int numParityUnits) {
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
     * @param k erased node index.
     * @return all the planes which have a hole-dot pair at k.
     */
    public int[] getHelperPlanesIndexes(int k) {

        int[] a = getNodeCoordinates(k);
        int x = a[0];
        int y = a[1];

        int exp = (int) Math.pow(q,t-1);
        int zIndexes[] = new int[exp];

        int j=0;
        for (int i=0; i< ((int)Math.pow(q,t)); i++) {
            int[] zVector = getZVector(i);
            if(zVector[y] == x)
                zIndexes[j++] = i;
        }

        return zIndexes;
    }
}
