import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LRUBufferManagerTest {
    private LRUBufferManager bufferManager;
    private int bufferSize = 5;

    @Before
    public void setUp() {
        bufferManager = new LRUBufferManager(bufferSize, 4, "LRUBufferManagerTestFile.bin", false);
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

    // Test to check if exception is thrown when trying to get a page with invalid page id
    @Test(expected = IllegalArgumentException.class)
    public void testGetPageWithInvalidPageId() {
        bufferManager.getPage(1000);
    }
}