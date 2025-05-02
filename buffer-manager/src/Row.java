import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Row {

    private ByteBuffer dataBuffer;
    private int startIndex;
    private int length;

    public Row(ByteBuffer buffer) {
        if (buffer.capacity() > 100) {
            dataBuffer = ByteBuffer.allocate(buffer.remaining());
            dataBuffer.put(buffer);
            startIndex = 0;
            length = dataBuffer.capacity();
        } else {
            dataBuffer = buffer;
            startIndex = buffer.position();
            length = buffer.remaining();
        }
    }

    public Row(byte[]... columns) {
        byte[] data;
        if (columns.length == 1) {
            data = columns[0];
        } else {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            try {
                for (byte[] col : columns) {
                    stream.write(col);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            data = stream.toByteArray();
        }
        dataBuffer = ByteBuffer.wrap(data);
        startIndex = 0;
        length = data.length;
    }

    public int length() {
        return length;
    }

    private ByteBuffer select(int[] range) {
        int start = (range.length < 1) ? 0 : range[0];
        int end = (range.length < 2) ? length : range[1];
        if (0 > start || start > end || end > length) {
            throw new IllegalArgumentException("Invalid Range for Row: [" + start + ", " + end + ")");
        }
        dataBuffer.clear();
        dataBuffer.position(startIndex + start);
        dataBuffer.limit(startIndex + end);
        return dataBuffer;
    }

    // prefer getRange over this
    public byte[] getAttribute(int attS, int attL) {
        return Arrays.copyOfRange(dataBuffer.array(), startIndex + attS, startIndex + attS + attL);
    }

    public ByteBuffer getRange(int... range) {
        return select(range).duplicate();
    }

    public int getInt(int... range) {
        return select(range).getInt();
    }

    public String getString(int... range) {
        return StandardCharsets.UTF_8.decode(select(range)).toString();
    }

    @Override
    public String toString() {
        return getString(); // range defaults to [0, length)
    }
    
}
