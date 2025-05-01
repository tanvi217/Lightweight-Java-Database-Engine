import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Row {

    private ByteBuffer dataBuffer;
    private int startIndex;
    private int length;

    public Row(ByteBuffer buffer) {
        dataBuffer = buffer;
        startIndex = buffer.position();
        length = buffer.remaining();
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

    private int[] verifyRange(int[] range) {
        if (range.length == 0) {
            return new int[] {0, length};
        }
        if (range.length == 1 && 0 <= range[0] && range[0] <= length) {
            return new int[] {range[0], length};
        }
        if (range.length < 2 || range[0] < 0 || range[1] > length || range[1] < range[0]) {
            throw new IllegalArgumentException("Invalid Range for Row");
        }
        return range;
    }

    public ByteBuffer getRange(int... range) {
        range = verifyRange(range);
        dataBuffer.position(startIndex + range[0]);
        dataBuffer.limit(startIndex + range[1]);
        return dataBuffer;
    }

    public String getString(int... range) {
        return StandardCharsets.UTF_8.decode(getRange(range)).toString();
    }

    public int getInt(int... range) {
        return getRange(range).getInt();
    }

    public int length() {
        return length;
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
