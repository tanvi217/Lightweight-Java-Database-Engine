import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;

public class IMDbPageTest {

    private TabularPage page;
    private ByteBuffer buffer;
    private final int pageBytes = 400; // Example page size, want to try something different than usual 4096
    private final int maxRows = (pageBytes - 8) / Constants.IMDB_ROW_LENGTH;
    private Row mockRow1;
    private Row mockRow2;

    @Before
    public void setUp() {
        //setup for tests
        buffer = ByteBuffer.allocate(1000); // Large enough buffer for multiple smaller pages
        int rowLength = Constants.IMDB_MOVIE_ID_SIZE + Constants.IMDB_TITLE_SIZE;
        page = new TabularPage(1, 0, pageBytes, buffer, rowLength);

        // Initialize mock rows with valid movieId and title
        byte[] movieId1 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}; // 9-byte movieId
        byte[] title1 = new byte[] {'T', 'i', 't', 'l', 'e', '1'}; // Shorter title

        byte[] movieId2 = new byte[] {9, 8, 7, 6, 5, 4, 3, 2, 1}; // Another movieId
        byte[] title2 = new byte[] {'M', 'o', 'v', 'i', 'e', '2'}; // Shorter title

        mockRow1 = new Row(movieId1, title1);
        mockRow2 = new Row(movieId2, title2);
    }

    // Test IMDbPage constructor and getId()
    @Test
    public void testConstructor() {
        //tests that we properly set up a page object and can get the ID.
        assertEquals("Page ID should be 1", 1, page.getId());
    }

    // Test insertRow() and isFull()
    @Test
    public void testInsertRow() {
        //this test is important to make sure we can insert rows, but here we are verifying that the index returned is correct
        assertEquals("First row should be inserted at index 0", 0, page.insertRow(mockRow1));
        assertEquals("Second row should be inserted at index 1", 1, page.insertRow(mockRow2));

        // Fill the page up to maxRows
        for (int i = 2; i < maxRows; i++) {
            //here we make sure we can add rows all the way up until maxRows
            assertNotEquals("Rows should be inserted successfully", -1, page.insertRow(mockRow1));
        }
        //here we make sure the page is full, which should be reflected in page.isFull() and .insertRow
        assertTrue("Page should be full", page.isFull());
        assertEquals("Inserting beyond capacity should return -1", -1, page.insertRow(mockRow2));
    }

    // Test retrieving inserted rows
    @Test
    public void testGetRow() {
        //here we test that the rows we insert are actually getting their data saved within the object somehow and that data can be retrieved correctly

        page.insertRow(mockRow1);
        page.insertRow(mockRow2);

        Row retrievedRow1 = page.getRow(0);
        Row retrievedRow2 = page.getRow(1);

        assertEquals("Retrieved movieId should match mockRow1", mockRow1.toString(), retrievedRow1.toString());

        assertEquals("Retrieved movieId should match mockRow2", mockRow2.toString(), retrievedRow2.toString());

        assertEquals("Row objects themselves should be equal", mockRow1, retrievedRow1);
        assertEquals("Row objects themselves should be equal", mockRow2, retrievedRow2);

        assertThrows(IllegalArgumentException.class, () -> page.getRow(2));
    }

    // Test handling out-of-bounds row retrieval
    @Test
    public void testGetRowOutOfBounds() {
        //Make sure that out of bounds indices throw exceptions, i.e. too high or low indexing.
        assertThrows("Negative index should throw exception", IllegalArgumentException.class, () -> page.getRow(-1));
        assertThrows("Index beyond nextRowId should throw exception", IllegalArgumentException.class, () -> page.getRow(0));
        page.insertRow(mockRow1);
        assertThrows("Index beyond nextRowId should throw exception", IllegalArgumentException.class, () -> page.getRow(1));
        //we also want to make sure this gives us problems at the tightest test cases and try after inserting a row as well.
    }

    // Test nextRowId consistency
    @Test
    public void testNextRowIdConsistency() {
        //this has to do with how we are storing data in the buffer. We are using the last byte to store the nextRowId to make for
        //efficient reading of pages from disk, this helps us loop over the proper number of rows when we pull out the data from disk.
        assertEquals("Initial nextRowId should be 0", 0, buffer.get(pageBytes - 1));
        page.insertRow(mockRow1);
        assertEquals("NextRowId should be updated to 1", 1, buffer.get(pageBytes - 1));
    }

    // Test isFull() method
    @Test
    public void testIsFull() {
        //another isFull test just to be sure.
        assertFalse("Page should not be full initially", page.isFull());

        for (int i = 0; i < maxRows; i++) {
            page.insertRow(mockRow1);
        }
        assertTrue("Page should be full after maxRows insertions", page.isFull());
    }
}
