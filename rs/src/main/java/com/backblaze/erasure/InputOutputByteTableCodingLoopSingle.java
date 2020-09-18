package com.backblaze.erasure;

public class InputOutputByteTableCodingLoopSingle {
    public void codeSomeShards(
        byte[][] matrixRows,
        byte[] input, int index,
        byte[] output, int outputIndex,
        int offset, int byteCount, boolean isFirstTime) {

        final byte [] [] table = Galois.MULTIPLICATION_TABLE;
        final byte[] matrixRow = matrixRows[outputIndex];
        final byte[] multTableRow = table[matrixRow[index] & 0xFF];
        for (int iByte = offset; iByte < offset + byteCount; iByte++) {
            if (isFirstTime) {
                output[iByte] = multTableRow[input[iByte] & 0xFF];
            } else {
                output[iByte] ^= multTableRow[input[iByte] & 0xFF];
            }
        }
    }
}