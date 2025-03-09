import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class BinaryPageTest {
    
    private BinaryPage page;
    private Row mockRow1;
    private Row mockRow2;

    @Before
    public void setUp() {
        // Initialize mock rows with valid movieId and title
        byte[] movieId1 = new byte[] {1, 2, 3}; // Example movieId (3 bytes)
        byte[] title1 = new byte[] {'T', 'h', 'e', ' ', 'M', 'o', 'v', 'i', 'e'}; // Example title (9 characters)

        byte[] movieId2 = new byte[] {4, 5, 6}; // Another example movieId (3 bytes)
        byte[] title2 = new byte[] {'A', 'n', 'o', 't', 'h', 'e', 'r', ' ', 'M', 'o', 'v', 'i', 'e'}; // Another title (13 characters)

        // Create mock Row objects
        mockRow1 = new Row(movieId1, title1);
        mockRow2 = new Row(movieId2, title2);

        // Initialize the BinaryPage with a given pid
        page = new BinaryPage(1);
    }

    // Test the constructor and getId method
    @Test
    public void testConstructor() {
        assertEquals("Page ID should be 1", 1, page.getId());
    }

    // Test the insertRow method and the isFull method
    @Test
    public void testInsertRow() {
        assertEquals("First row should be inserted at index 0", 0, page.insertRow(mockRow1));
        assertEquals("Second row should be inserted at index 1", 1, page.insertRow(mockRow2));
        //Try inserting the exact right amount of rows
        for (int i = 0; i < Constants.MAX_ROWS - 3; i++) {
            page.insertRow(mockRow1); // Insert mockRow1 multiple times
        }
        //returning row id so not 104 but 103
        assertEquals("Insert should work when page is near full", 103, page.insertRow(mockRow2));
        //try inserting more than maxrows
        assertEquals("Insert should fail when page is full", -1, page.insertRow(mockRow2));
    }

    // Test the getRow method
    @Test
    public void testGetRow() {
        page.insertRow(mockRow1);
        page.insertRow(mockRow2);

        assertEquals("Get the first row", mockRow1, page.getRow(0));
        assertEquals("Get the second row", mockRow2, page.getRow(1));
        assertEquals("Get the first row", mockRow1.getMovieId(), page.getRow(0).getMovieId());
        assertEquals("Get the second row", mockRow1.getTitle(), page.getRow(0).getTitle());
        assertEquals("Get the first row", mockRow2.getMovieId(), page.getRow(1).getMovieId());
        assertEquals("Get the second row", mockRow2.getTitle(), page.getRow(1).getTitle());
        assertNull("Get a non-existent row", page.getRow(2)); // Out of bounds
    }

    // Test the serialize method (test serialization of page)
    @Test
    public void testSerialize() {
        page.insertRow(mockRow1);
        page.insertRow(mockRow2);

        byte[] serializedData = page.serialize();
        assertNotNull("Serialized data should not be null", serializedData);
        assertEquals("Serialized data length should match PAGE_SIZE", Constants.PAGE_SIZE, serializedData.length);
    }

    // Test the deserialize method (test deserialization of page)
    @Test
    public void testDeserialize() {
        page.insertRow(mockRow1);
        page.insertRow(mockRow2);

        byte[] serializedData = page.serialize();

        BinaryPage newPage = new BinaryPage(2);
        assertTrue("Deserialization should succeed", newPage.deserialize(serializedData));
        System.out.println(newPage.getRow(0));
        System.out.println(newPage.getRow(2));
        //assertEquals("Deserialized page should match row count", 2, newPage.g);
    }

    // Test the deserialize method with invalid data
     
    @Test
    public void testDeserializeInvalidData() {
        byte[] invalidData = new byte[4]; // Simulate invalid data (wrong size)
        BinaryPage newPage = new BinaryPage(2);

        assertFalse("Deserialization should fail for invalid data", newPage.deserialize(invalidData));
    }
    
    // Test the isFull method
    @Test
    public void testIsFull() {
        page.insertRow(mockRow1);
        assertFalse("Page should not be full yet", page.isFull());

        for (int i = 0; i < Constants.MAX_ROWS - 1; i++) {
            page.insertRow(mockRow2);
        }
        assertTrue("Page should be full", page.isFull());
    }
}

