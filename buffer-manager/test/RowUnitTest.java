import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import java.nio.charset.StandardCharsets;

public class RowUnitTest {

    private Row row;
    private Row properSizeRow;
    private byte[] movieId;
    private byte[] title;
    private byte[] movieId2;
    private byte[] title2;

    @Before
    public void setUp() {
        movieId = "123456789".getBytes(StandardCharsets.UTF_8);
        title = "Example Movie".getBytes(StandardCharsets.UTF_8);
        row = new Row(movieId, title);
        movieId2 = "123456788".getBytes(StandardCharsets.UTF_8);
        title2 = "This is thirty characters67890".getBytes(StandardCharsets.UTF_8);
        row = new Row(movieId, title);
        properSizeRow = new Row(movieId2, title2);
    }

    @Test
    public void testConstructor() {
        //tests that constructor properly saves values.
        assertArrayEquals("MovieId should be correctly initialized", movieId, row.movieId);
        assertArrayEquals("Title should be correctly initialized", title, row.title);
    }

    @Test
    public void testGetMovieId() {
        //tests that get movieId returns proper string.
        assertEquals("MovieId should be correctly retrieved", "123456789", row.getMovieId());
    }

    @Test
    public void testGetTitle() {
        //tetss that getTitle returns proper string
        assertEquals("Title should be correctly retrieved", "Example Movie", row.getTitle());
    }

    @Test
    public void testSerialize() {
        //tests that serialize gives back data of the proper size, and makes sure that serialize data is not null. 
        //This function is not used in our final implementation, however it is used in Binary Page which is another page implementation we 
        //created that we decided not to use but figured we would leave it in as it shows another way we could store data.
        byte[] serialized = row.serialize();
        assertNotNull("Serialized data should not be null", serialized);
        assertEquals("Serialized data should have correct length", Constants.ROW_SIZE, serialized.length);
    }

    @Test
    public void testDeserialize() {
        //again not used anymore, but just testing the basic functionality of the function, this must take in a full row
        byte[] serialized = properSizeRow.serialize();
        Row deserializedRow = Row.deserialize(serialized);
        assertEquals("Deserialized MovieId should match original", properSizeRow.getMovieId(), deserializedRow.getMovieId());
        assertEquals("Deserialized Title should match original", properSizeRow.getTitle(), deserializedRow.getTitle());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeWithInvalidData() {
        //makes sure deserialize does not work for wrong size.
        byte[] invalidData = new byte[10]; // Incorrect size
        Row.deserialize(invalidData);
    }
}
