import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Row {
    public byte[] movieId;
    public byte[] title;

    public Row() {}

    public Row(byte[] movieId, byte[] title) {
        //this.movieId = addPadding(movieId, Constants.MOVIE_ID_SIZE);
        //this.title = addPadding(title, Constants.TITLE_SIZE);
        this.movieId = movieId;
        this.title = title;
    }

    public String getMovieId(){
        return new String(this.movieId, StandardCharsets.UTF_8);
    }

    public String getTitle(){
        return new String(this.title, StandardCharsets.UTF_8);
    }

    private byte[] addPadding(byte[] input, int requiredLength) {
        byte[] result = new byte[requiredLength];
        int lengthToCopy = Math.min(input.length, requiredLength);
        System.arraycopy(input, 0, result, 0, lengthToCopy);

        if (input.length < requiredLength) {
            Arrays.fill(result, input.length, requiredLength, (byte) 0);
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
        validateDataLength(data);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] movieId = extractBytes(buffer, Constants.MOVIE_ID_SIZE);
        byte[] title = extractBytes(buffer, Constants.TITLE_SIZE);

        return new Row(movieId, title);
    }

    private static void validateDataLength(byte[] data) {
        if (data.length != Constants.ROW_SIZE) {
            throw new IllegalArgumentException("Row data must be exactly 39 bytes");
        }
    }

    private static byte[] extractBytes(ByteBuffer buffer, int size) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return bytes;
    }
}