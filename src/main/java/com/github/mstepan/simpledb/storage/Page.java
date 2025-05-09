package com.github.mstepan.simpledb.storage;

import static com.github.mstepan.simpledb.util.Preconditions.checkArguments;
import static com.github.mstepan.simpledb.util.Preconditions.checkState;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.Date;

public final class Page {

    private static final byte BYTE_ZERO = 0;
    private static final byte BYTE_ONE = 1;

    private static final char CSTRING_TERMINATOR = '\0';

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

    public void putLong(int offset, long value) {
        checkBoundary(offset + Long.BYTES);
        buf.putLong(offset, value);
    }

    public long getLong(int offset) {
        checkBoundary(offset + Long.BYTES);
        return buf.getLong(offset);
    }

    public void putInt(int offset, int value) {
        checkBoundary(offset + Integer.BYTES);
        buf.putInt(offset, value);
    }

    public int getInt(int offset) {
        checkBoundary(offset + Integer.BYTES);
        return buf.getInt(offset);
    }

    public void putChar(int offset, char value) {
        checkBoundary(offset + Character.BYTES);
        buf.putChar(offset, value);
    }

    public char getChar(int offset) {
        checkBoundary(offset + Character.BYTES);
        return buf.getChar(offset);
    }

    public void putBoolean(int offset, boolean value) {
        checkBoundary(offset + Character.BYTES);
        buf.put(offset, value ? BYTE_ZERO : BYTE_ONE);
    }

    public boolean getBoolean(int offset) {
        checkBoundary(offset + Character.BYTES);
        return buf.get(offset) == BYTE_ZERO;
    }

    public void putBytes(int offset, byte[] src) {
        checkArguments(src != null, "null 'src' byte[] array detected");
        checkBoundary(offset + Integer.BYTES + src.length);
        buf.position(offset);
        buf.putInt(src.length);
        buf.put(src);
    }

    public byte[] getBytes(int offset) {
        checkBoundary(offset + Integer.BYTES);

        buf.position(offset);
        int len = buf.getInt();

        byte[] dest = new byte[len];
        buf.get(dest);

        return dest;
    }

    public void putString(int offset, String str) {
        checkArguments(str != null, "null 'str' detected");
        putBytes(offset, str.getBytes(DEFAULT_CHARSET));
    }

    public String getString(int offset) {
        byte[] rawBytes = getBytes(offset);
        return new String(rawBytes, DEFAULT_CHARSET);
    }

    /** Store string as C-string with '\0' terminated character at the end. */
    public void putStringC(int offset, String str) {
        checkArguments(str != null, "null 'str' detected");

        final CharsetEncoder charsEncoder = DEFAULT_CHARSET.newEncoder();
        int bytesPerChar = (int) charsEncoder.maxBytesPerChar();
        int charsToWrite = str.length() + 1;

        checkBoundary(offset + bytesPerChar * charsToWrite);

        try {
            ByteBuffer encodedData = charsEncoder.encode(CharBuffer.wrap(str + CSTRING_TERMINATOR));
            buf.put(offset, encodedData.array());
        } catch (CharacterCodingException encodingEx) {
            throw new IllegalStateException(encodingEx);
        }
    }

    public String getStringC(int offset) {
        StringBuilder data = new StringBuilder();

        final CharsetDecoder charsDecoder = DEFAULT_CHARSET.newDecoder();
        CharBuffer charBuffer = CharBuffer.allocate(1);

        buf.position(offset);

        while (true) {
            checkState(
                    offset < blockSize,
                    "'offset' is out of bounds for Page, offset = %d, page boundary = %d"
                            .formatted(offset, blockSize));

            CoderResult result = charsDecoder.decode(buf, charBuffer, false);

            checkState(!result.isError(), "Decoding character error");

            if (charBuffer.position() > 0) {
                charBuffer.flip();
                char decodedSingleCh = charBuffer.get();

                if (decodedSingleCh == CSTRING_TERMINATOR) {
                    break;
                }

                data.append(decodedSingleCh);
                charBuffer.clear();
            }
        }

        return data.toString();
    }

    public void putDate(int offset, Date value) {
        putLong(offset, value.getTime());
    }

    public Date getDate(int offset) {
        return new Date(getLong(offset));
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
