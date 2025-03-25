import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

public class LRUBufferManagerTest {
    private LRUBufferManager bufferManager;
    private int bufferSize = 2;
    private String testFileName = "LRUBufferManagerTestFile.bin";

    @Before
    public void setUp() {
        bufferManager = new LRUBufferManager(bufferSize, 4, testFileName, false);
    }

    @After
    public void tearDown() {
        File testFile = new File(testFileName);

        if (testFile.exists()) {
            testFile.delete();
        }
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

    // Test to check if exception is thrown when unpinning a page with invalid page id
    @Test(expected = IllegalArgumentException.class)
    public void testUnpinPageWithInvalidPageId() {
        bufferManager.unpinPage(1001);
    }

    // Test to check if exception is thrown when marking a page dirty with invalid page id
    @Test(expected = IllegalArgumentException.class)
    public void testMarkPageDirtyWithInvalidPageId() {
        bufferManager.markDirty(1001);
    }

    // Test to check if exception is thrown when a page is created with full buffer and all pages are pinned
    @Test(expected = IllegalStateException.class)
    public void testAllPagesPinned() {
        for (int i = 0; i < bufferSize; i++) {
            bufferManager.createPage();
        }

        bufferManager.createPage();
    }

    // Checks if page is created and evicted based on LRU policy, file is created and has data
    @Test
    public void testCreateAndEvictPages() throws Exception {
        Page p1 = bufferManager.createPage();
        assertNotNull(p1);

        Row insertedRow = new Row("tt0000001".getBytes(), "Movie 1".getBytes());
        p1.insertRow(insertedRow);
        bufferManager.markDirty(p1.getId());

        Page p2 = bufferManager.createPage();
        assertNotNull(p2);
        p2.insertRow(new Row("tt0000002".getBytes(), "Movie 2".getBytes()));

        bufferManager.unpinPage(p1.getId());

        // file should be empty since no page is evicted yet
        File file = new File(testFileName);
        assertTrue(file.length() == 0);

        Page p3 = bufferManager.createPage(); // since buffer pool is full, p1 should be evicted based on LRU
        assertNotNull(p3);

        // p1 should be evicted and written to file
        assertTrue(file.length() > 0);
    }
}