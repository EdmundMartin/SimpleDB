package SimpleDB.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {

    private final ByteBuffer buffer;
    public static Charset CHARSET = StandardCharsets.US_ASCII;

    // For creating data buffers
    public Page(int blockSize) {
        this.buffer = ByteBuffer.allocateDirect(blockSize);
    }

    // For creating log pages
    public Page(byte[] bytes) {
        this.buffer = ByteBuffer.wrap(bytes);
    }

    public int getInt(int offset) {
        return this.buffer.getInt(offset);
    }

    public void setInt(int offset, int value) {
        this.buffer.putInt(offset, value);
    }

    public byte[] getBytes(int offset) {
        this.buffer.position(offset);
        int length = this.buffer.getInt();
        byte[] b = new byte[length];
        this.buffer.get(b);
        return b;
    }

    public void setBytes(int offset, byte[] bytes) {
        this.buffer.position(offset);
        this.buffer.putInt(bytes.length);
        this.buffer.put(bytes);
    }

    public String getString(int offset) {
        byte[] bytes = getBytes(offset);
        return new String(bytes, CHARSET);
    }

    public void setString(int offset, String s) {
        byte[] bytes = s.getBytes(CHARSET);
        setBytes(offset, bytes);
    }

    public static int maxLength(int strLength) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strLength * (int) bytesPerChar);
    }

    // package private method, needed by FileMgr
    ByteBuffer contents() {
        this.buffer.position(0);
        return this.buffer;
    }

}
