import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Row implements Comparable<Row> {

    public byte[] data;
    private ByteBuffer dataBuffer;

    public Row(ByteBuffer dataBuffer) {
        data = dataBuffer.array();
        this.dataBuffer = dataBuffer;
    }

    public Row(byte[]... columns) {
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

    public byte[] getAttribute(int attS, int attL) {
        return Arrays.copyOfRange(data, attS, attS + attL);
    }

    private int[] verifyRange(int[] range) {
        if (range.length == 0) {
            return new int[] {0, dataBuffer.capacity()};
        }
        if (range.length == 1 && 0 <= range[0] && range[0] <= dataBuffer.capacity()) {
            return new int[] {range[0], dataBuffer.capacity()};
        }
        if (range.length < 2 || range[0] < 0 || range[1] > dataBuffer.capacity() || range[1] < range[0]) {
            throw new IllegalArgumentException("Invalid Range for Row");
        }
        return range;
    }

    public ByteBuffer getRange(int... range) {
        range = verifyRange(range);
        dataBuffer.position(range[0]);
        dataBuffer.limit(range[1]);
        return dataBuffer;
    }

    public byte[] getBytes(int bytesInRow) { // is this used?
        return Arrays.copyOf(dataBuffer.array(), bytesInRow);
    }

    public String getString(int... range) {
        return StandardCharsets.UTF_8.decode(getRange(range)).toString();
    }

    public int getInt(int... range) {
        return getRange(range).getInt();
    }

    public int compareTo(byte[] key, int... range) {
        range = verifyRange(range);
        return Arrays.compare(dataBuffer.array(), range[0], range[1], key, 0, key.length);
    }

    @Override
    public int compareTo(Row row) {
        return getRange().compareTo(row.getRange());
    }

    @Override
    public String toString() {
        return getString(); // range defaults to {0, dataBuffer.capacity()}
    }
    
}
