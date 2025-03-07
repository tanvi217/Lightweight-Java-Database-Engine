import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Row {
    public static final int MOVIE_ID_SIZE = 9;  // movieId size: 9
    public static final int TITLE_SIZE = 30;    // title size: 30
    public static final int ROW_SIZE = MOVIE_ID_SIZE + TITLE_SIZE; // each row size

    public byte[] movieId;
    public byte[] title;

    public Row(byte[] movieId, byte[] title) {
    this.movieId = new byte[MOVIE_ID_SIZE];
    this.title = new byte[TITLE_SIZE];

    // Copy movieId within the 9-byte fized size
    System.arraycopy(movieId, 0, this.movieId, 0, Math.min(movieId.length, MOVIE_ID_SIZE));

    // Trim or pad title to exactly 30 bytes
    byte[] tempTitle = Arrays.copyOf(title, TITLE_SIZE); //pads with 0s if shorter than 30
    this.title = tempTitle;
}

//byte array to write row to pages
public byte[] serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(ROW_SIZE); //create 39 byte buffer
    buffer.put(movieId, 0, MOVIE_ID_SIZE);  //movieId into buffer
    buffer.put(title, 0, TITLE_SIZE);  // title into buffer
    return buffer.array();
}

public static Row deserialize(byte[] data) {
    ByteBuffer buffer = ByteBuffer.wrap(data);
    byte[] movieId = new byte[MOVIE_ID_SIZE]; 
    byte[] title = new byte[TITLE_SIZE];
    buffer.get(movieId);//read first 9 bytes to movieId
    buffer.get(title); //read next 30 bytes to movieId
    return new Row(movieId, title);
}


//debug
@Override
public String toString() {
    return "Movie ID: " + new String(movieId, StandardCharsets.UTF_8).trim() +
            ", Title: " + new String(title, StandardCharsets.UTF_8).trim();
}
}