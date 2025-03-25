import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import java.nio.charset.StandardCharsets;

public class RowUnitTest {

    private Row row;
    private byte[] movieId;
    private byte[] title;

    @Before
    public void setUp() {
        movieId = "123456789".getBytes(StandardCharsets.UTF_8);
        title = "Example Movie".getBytes(StandardCharsets.UTF_8);
        row = new Row(movieId, title);
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
        //tests that getTitle returns proper string
        assertEquals("Title should be correctly retrieved", "Example Movie", row.getTitle());
    }

}
