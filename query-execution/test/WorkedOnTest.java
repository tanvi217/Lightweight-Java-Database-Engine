import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class WorkedOnTest {
    private BufferManager bufferManager;
    private WorkedOn workedOn;

    @Before
    public void setUp() {
        bufferManager = new LRUBufferManager(2, Constants.PAGE_KB, false);
        workedOn = new WorkedOn(bufferManager);
    }

    @Test
    public void testInsertAndRetrieveRows() {
        Page page = workedOn.createPage();
        Row row = new Row(
            new byte[] { 't', 't', '0', '0', '0', '0', '0', '0', '1' },
            new byte[] { 'p', 'e', 'r', 's', 'o', 'n', '0', '0', '0', '1' },
            new byte[] { 'd', 'i', 'r', 'e', 'c', 't', 'o', 'r' } 
        );
        page.insertRow(row);

        Row retrievedRow = page.getRow(0);
        assertNotNull(retrievedRow);

        byte[] movieId = retrievedRow.getAttribute(0, 9);
        assertArrayEquals(new byte[] { 't', 't', '0', '0', '0', '0', '0', '0', '1' }, movieId);

        byte[] personId = retrievedRow.getAttribute(9, 10);
        assertArrayEquals(new byte[] { 'p', 'e', 'r', 's', 'o', 'n', '0', '0', '0', '1' }, personId);

        byte[] category = retrievedRow.getAttribute(19, 39);
        String categoryString = new String(category).trim();
        assertTrue(categoryString.contains("director"));
    }
}