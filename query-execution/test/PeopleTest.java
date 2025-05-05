import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class PeopleTest {
    private BufferManager bufferManager;
    private People people;

    @Before
    public void setUp() {
        bufferManager = new LRUBufferManager(2, Constants.PAGE_KB, false);
        people = new People(bufferManager);
    }

    @Test
    public void testInsertAndRetrieveRows() {
        Page page = people.createPage();
        Row row = new Row(
            new byte[] { 'p', 'e', 'r', 's', 'o', 'n', '0', '0', '0', '1' },
            new byte[] { 'J', 'o', 'h', 'n' }
        );
        page.insertRow(row);

        Row retrievedRow = page.getRow(0);
        assertNotNull(retrievedRow);

        byte[] personId = retrievedRow.getAttribute(0, 10);
        assertArrayEquals(new byte[] { 'p', 'e', 'r', 's', 'o', 'n', '0', '0', '0', '1' }, personId);

        byte[] name = retrievedRow.getAttribute(10, 115);
        String nameString = new String(name).trim();
        assertTrue(nameString.contains("John"));
    }
}