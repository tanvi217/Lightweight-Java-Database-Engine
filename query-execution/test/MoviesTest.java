import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class MoviesTest {
    private BufferManager bufferManager;
    private Movies movies;

    @Before
    public void setUp() {
        bufferManager = new LRUBufferManager(2, Constants.PAGE_KB, false);

        movies = new Movies(bufferManager);
    }

    @Test
    public void testInsertAndRetrieveRows() {
        Page page = movies.createPage();
        Row row = new Row(new byte[] { 't', 't', '0', '0', '0', '0', '0', '0', '1' },
                          new byte[] { 'M', 'o', 'v', 'i', 'e' });
        page.insertRow(row);

        Row retrievedRow = page.getRow(0);
        assertNotNull(retrievedRow);

        byte[] movieId = retrievedRow.getAttribute(0, 9);
        assertArrayEquals(new byte[] { 't', 't', '0', '0', '0', '0', '0', '0', '1' }, movieId);

        byte[] title = retrievedRow.getAttribute(9, 39);
        String titleString = new String(title).trim(); // Convert byte[] to String and trim whitespace
        assertTrue(titleString.contains("Movie"));
    }
}