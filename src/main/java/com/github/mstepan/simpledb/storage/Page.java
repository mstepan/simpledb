package com.github.mstepan.simpledb.storage;

import static com.github.mstepan.simpledb.util.Preconditions.checkArguments;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class Page {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.US_ASCII;
    private final int blockSize;
    private final ByteBuffer buf;

    public Page(int blockSize) {
        checkArguments(
                blockSize >= 0,
                "'blockSize' must be >= 0', current value: %d".formatted(blockSize));
        this.blockSize = blockSize;
        buf = ByteBuffer.allocateDirect(blockSize);
    }

    Page(byte[] arr) {
        checkArguments(arr != null, "null 'arr' detected");
        this.blockSize = arr.length;
        buf = ByteBuffer.wrap(arr);
    }

    /** Reset internal ByteBuffer position and return. */
    ByteBuffer content() {
        buf.position(0);
        return buf;
    }

    public long getLong(int offset) {
        checkBoundary(offset + Long.BYTES);
        return buf.getLong(offset);
    }

    public void putLong(int offset, long value) {
        checkBoundary(offset + Long.BYTES);
        buf.putLong(offset, value);
    }

    public int getInt(int offset) {
        checkBoundary(offset + Integer.BYTES);
        return buf.getInt(offset);
    }

    public void putInt(int offset, int value) {
        checkBoundary(offset + Integer.BYTES);
        buf.putInt(offset, value);
    }

    public char getChar(int offset) {
        checkBoundary(offset + Character.BYTES);
        return buf.getChar(offset);
    }

    public void putChar(int offset, char value) {
        checkBoundary(offset + Character.BYTES);
        buf.putChar(offset, value);
    }

    public byte[] getBytes(int offset) {
        checkBoundary(offset + Integer.BYTES);

        buf.position(offset);
        int len = buf.getInt();

        byte[] dest = new byte[len];
        buf.get(dest);

        return dest;
    }

    public void putBytes(int offset, byte[] src) {
        checkBoundary(offset + Integer.BYTES + src.length);
        buf.position(offset);
        buf.putInt(src.length);
        buf.put(src);
    }

    public String getString(int offset) {
        byte[] rawBytes = getBytes(offset);
        return new String(rawBytes, DEFAULT_CHARSET);
    }

    public void putString(int offset, String str) {
        putBytes(offset, str.getBytes(DEFAULT_CHARSET));
    }

    public static int strLengthInBytes(int strLength) {
        final int bytesPerCh = (int) DEFAULT_CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strLength * bytesPerCh);
    }

    private void checkBoundary(int boundaryOffset) {
        if (boundaryOffset < 0 || boundaryOffset > blockSize) {
            throw new IndexOutOfBoundsException(
                    ("Can't store value out of Page boundary, "
                                    + "page size = %d and boundaryOffset = %d")
                            .formatted(blockSize, boundaryOffset));
        }
    }
}
