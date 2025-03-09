import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class BufferManagerLRUTest {
    private BufferManagerLRU bufferManager;

    @Before
    public void setUp() {
        bufferManager = new BufferManagerLRU(2, false);
    }

    @Test
    public void testCreatePage() {
        Page page = bufferManager.createPage();
        assertNotNull(page);
        assertEquals(0, page.getId());
    }

    @Test
    public void testGetPage() {
        Page page = bufferManager.createPage();
        Page retrievedPage = bufferManager.getPage(page.getId());
        assertNotNull(retrievedPage);
        assertEquals(page.getId(), retrievedPage.getId());
    }
}