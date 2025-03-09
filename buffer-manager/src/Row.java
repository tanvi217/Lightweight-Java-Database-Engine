import java.nio.ByteBuffer;
import java.util.Arrays;

public class Row {
    private byte[] movieId;
    private byte[] title;

    public Row(byte[] movieId, byte[] title) {
        this.movieId = addPadding(movieId, Constants.MOVIE_ID_SIZE);
        this.title = addPadding(title, Constants.TITLE_SIZE);
    }

    // Pad or truncate to fixed size
    private byte[] addPadding(byte[] input, int targetLength) {
        byte[] result = new byte[targetLength];
        System.arraycopy(input, 0, result, 0, Math.min(input.length, targetLength));

        if (input.length < targetLength) {
            Arrays.fill(result, input.length, targetLength, (byte) 0);
        }
    
        return result;
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Constants.ROW_SIZE);

        buffer.put(movieId);
        buffer.put(title);

        return buffer.array();
    }

    public static Row deserialize(byte[] data) {
        if (data.length != Constants.ROW_SIZE) {
            throw new IllegalArgumentException("Row data must be exactly 39 bytes");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] movieId = new byte[Constants.MOVIE_ID_SIZE];
        byte[] title = new byte[Constants.TITLE_SIZE];
        buffer.get(movieId);
        buffer.get(title);

        return new Row(movieId, title);
    }
}