import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Row {

    private static ByteBuffer allocateCopy(ByteBuffer src) {
        return ByteBuffer.allocate(src.remaining()).put(src).clear();
    }

    private ByteBuffer dataBuffer;

    /**
     * A row created in this way is a subsection of some potentially larger ByteBuffer. The backing array will be shared unless the boolean 'copy' is true. The new buffer has initial position = 0 and capacity = buffer.remaining()
     */
    public Row(ByteBuffer data, boolean copy) {
        dataBuffer = copy ? allocateCopy(data) : data.slice();
    }

    /**
     * The constructor with only a ByteBuffer passed defaults to not copying the backing content.
     */
    public Row(ByteBuffer data) {
        dataBuffer = data.slice();
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
    }

    public int length() {
        return dataBuffer.capacity();
    }

    public byte[] getAttribute(int attS, int attL) {
        int start = dataBuffer.arrayOffset() + attS;
        int end =  dataBuffer.arrayOffset() + attS + attL;
        return Arrays.copyOfRange(dataBuffer.array(), start, end);
    }

    private ByteBuffer select(int... range) {
        int start = (range.length < 1) ? 0 : range[0];
        int end = (range.length < 2) ? dataBuffer.capacity() : range[1];
        if (0 > start || start > end || end > dataBuffer.capacity()) {
            throw new IllegalArgumentException("Invalid Range for Row: [" + start + ", " + end + ")");
        }
        return dataBuffer.clear().position(start).limit(end);
    }

    public Row copy() {
        return new Row(select(), true);
    }

    /**
     * Returns a new ByteBuffer with the same backing array. Content is subject to changes.
     */
    public ByteBuffer viewRange(int... range) {
        return select(range).slice();
    }

    /**
     * Returns a new ByteBuffer with a new backing array, such that future changes to the dataBuffer backing array will not affect the new ByteBuffer.
     */
    public ByteBuffer copyRange(int... range) {
        return allocateCopy(select(range));
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
