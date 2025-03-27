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

    public static void bufferManagerLRUTests(){
        //This test is useful as we run on a small set of pages and make sure the basic functionality works
        //We are able to add pages to the buffer manager, keep them in the frames, evict to add new ones, and write to disk
        //The disk writes are all 0 as each page has not had any information added yet, but importantly in imdb_db.bin we can
        //see that there are 6000 lines (in hex) i.e. 6 pages worth of space. We do this using the hex editor installed on vscode
        int totalPages = 10;
        int bufferSize = 4;
        BufferManager bm = new LRUBufferManager(bufferSize, true);
        for (int i = 0; i < totalPages; ++i) {
            Page page = bm.createPage();
            System.out.println("A" + page.getId());
            byte[] id = "tt0000001".getBytes();
            byte[] title = "Carmencita".getBytes();
            Row r = new Row(id, title);
            page.insertRow(r);
            bm.unpinPage(page.getId());
            System.out.println("got page " + i + ": " + bm.toString());
        }

        //now after we added some pages let's actually try writing something to them...
        Page page = bm.getPage(0);
        Row currRow = page.getRow(0);
        System.out.println("HERE");
        System.out.println(currRow);
        
        byte[] id = "tt0000001".getBytes();
        byte[] title = "Carmencita".getBytes();
        Row r = new Row(id, title);
        page.insertRow(r);
        System.out.println(page.getRow(1));
        bm.markDirty(0);
        bm.unpinPage(0);
        bm.createPage();
        bm.createPage();
        bm.createPage();
        bm.createPage();
        //this forces us to write to disk and we see it looks good YAY
    }

}