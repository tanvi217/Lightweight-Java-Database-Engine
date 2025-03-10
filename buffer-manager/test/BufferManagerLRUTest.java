import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class BufferManagerLRUTest {
    private LRUBufferManager bufferManager;

    @Before
    public void setUp() {
        bufferManager = new LRUBufferManager(2, false);
    }

    @Test
    public void testCreatePage() {
        //barbones test to make sure creation of a page works
        Page page = bufferManager.createPage();
        assertNotNull(page);
        assertEquals(0, page.getId());
    }

    @Test
    public void testGetPage() {
        //checking a very basic get.
        Page page = bufferManager.createPage();
        Page retrievedPage = bufferManager.getPage(page.getId());
        assertNotNull(retrievedPage);
        assertEquals(page.getId(), retrievedPage.getId());
    }
}