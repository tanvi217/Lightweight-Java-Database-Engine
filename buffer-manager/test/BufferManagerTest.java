import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Comments are intended to give an idea of what the buffer might look like, but
 * the real contents could differ depending on implementation. Adapted to work with JUnit.
 */
public class BufferManagerTest {

    public static BufferManager getNewBM(int bufferSize) {
        // CHANGE TO WHICHEVER IMPLEMENTATION YOU WANT TO TEST
        return new LRUBufferManager(bufferSize, Constants.PAGE_KB, true);
    }

    private BufferManager bm;
    private static int numPages = (Constants.BUFFER_SIZE * 5) / 2;
    private static int pageBytes = Constants.PAGE_KB * 1024;
    private static int rowsPerPage = (pageBytes - 8) / Constants.IMDB_ROW_LENGTH; // 8 for two metadata ints
    private static int numRows = rowsPerPage * numPages;

    @Before
    public void initializeBM() throws IOException { // runs before each test case
        bm = getNewBM(Constants.BUFFER_SIZE);
    }

    @Test
    public void testCreatePage() throws IOException {
        //makes sure we can create numerous pages and check the id of the final page.
        Page p = null;
        for (int i = 0; i < Constants.BUFFER_SIZE; ++i) {
            p = bm.createPage();
        }
        assertNotNull(p);
        assertEquals(Constants.BUFFER_SIZE - 1, p.getId());
    }

    @Test
    public void testUnpinPage() throws IOException {
        //make sure that we can unpin pages without breaking anything, throwing errors or anything of that nature.
        //since we are always unpinning we should create all the pages without any errors happening.
        Page p = null;
        for (int i = 0; i < numPages; ++i) {
            p = bm.createPage();
            bm.unpinPage(p.getId());
        }
        assertNotNull(p);
        assertEquals(numPages - 1, p.getId());
    }

    private Row testRow(int pageId, int rowNumber, boolean correctSizes) {
        //function used to generate a row for testing purposes.
        int movieIdBytes = correctSizes ? Constants.IMDB_MOVIE_ID_SIZE : 4;
        int titleBytes = correctSizes ? Constants.IMDB_TITLE_SIZE : 4;
        byte[] movieId = ByteBuffer.allocate(movieIdBytes).putInt(pageId).array();
        byte[] title = ByteBuffer.allocate(titleBytes).putInt(rowNumber).array();
        return new Row(movieId, title);
    }

    private void populateBM() throws IOException {
        // function used to populate the buffer manager.
        Page p = bm.createPage();
        for (int i = 0; i < numRows; ++i) {
            if (p.isFull()) {
                bm.unpinPage(p.getId());
                p = bm.createPage();
            }
            p.insertRow(testRow(p.getId(), i, true));
        }
        bm.unpinPage(p.getId());
    }

    @Test
    public void testGetPage() throws IOException {
        populateBM();
        System.out.println(bm);
        int[] getIds = {18, 3, 2, 0, 13, 12, 19, 1, 6}; // all must be less than numPages
        //testing getting some arbitrary valid page Ids
        for (int i = 0; i < getIds.length; ++i) {
            Page p = bm.getPage(getIds[i]);
            assertNotNull(p);
            assertEquals(getIds[i], p.getId());
            Row first = p.getRow(0);
            int recoveredPageId = first.getInt();
            assertEquals(getIds[i], recoveredPageId);
            bm.unpinPage(p.getId());
        }
    }

    @Test
    public void threeOlderTestCases() {

        // test cases, each has no arguments and returns a string result
        @SuppressWarnings("unchecked")
        Supplier<String>[] tests = new Supplier[] {
                BufferManagerTest::overfillBuffer,
                BufferManagerTest::evictPage,
                BufferManagerTest::lruLongerTest
        };

        for (int i = 0; i < tests.length; ++i) {
            String result = tests[i].get();
            System.out.println("TEST " + (i + 1) + " RESULT: " + result);
            assertEquals("Passed", result.substring(0, 6));
        }

    }

    public static String overfillBuffer() {
        //checking to make sure if we overfill buffer WITHOUT unpinning we get an error as expected.
        int bufferSize = 3;
        int totalPages = 4;
        Page[] p = new Page[totalPages];
        BufferManager bm = getNewBM(bufferSize);
        boolean caughtError = false;
        try {
            // -,0  -,0  -,0
            p[0] = bm.createPage();
            p[1] = bm.createPage();
            p[2] = bm.createPage();
            // 0,1  1,1  2,1
            p[3] = bm.createPage();
        } catch (Exception e) {
            caughtError = true;
        }
        return "Passed overfillBuffer(), and expected 1 exceptions to be thrown. Caught status [" + caughtError + "]";
    }

    public static String evictPage() {
        //testing that we can evict a page in a similar instance to above but now we unpin ONE page.
        int bufferSize = 3;
        int totalPages = 4;
        Page[] p = new Page[totalPages];
        BufferManager bm = getNewBM(bufferSize);
        try {
            // -,0  -,0  -,0
            p[0] = bm.createPage();
            p[1] = bm.createPage();
            p[2] = bm.createPage();
            bm.unpinPage(p[1].getId());
            // 0,1  1,0  2,1
            p[3] = bm.createPage();
            // 0,1  3,1  2,1
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed due to unexpected error: " + e.toString();
        }
        return "Passed evictPage(), and expected no exceptions to be thrown.";
    }

    public static String lruLongerTest() {
        //a longer sequence of tests similar to the above to really make sure that eviction and overfilling work as anticipated.
        int bufferSize = 4;
        int totalPages = 8;
        Page[] p = new Page[totalPages];
        BufferManager bm = getNewBM(bufferSize);
        try {
            // -,0  -,0  -,0  -,0
            p[0] = bm.createPage();
            p[1] = bm.createPage();
            p[2] = bm.createPage();
            // 0,1  1,1  2,1  -,0
            bm.unpinPage(p[1].getId());
            // 0,1  1,0  2,1  -,0
            p[3] = bm.createPage();
            p[4] = bm.createPage();
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed due to unexpected error: " + e.toString();
        }
        boolean[] foundError = new boolean[3]; // update number of errors here
        try {
            // 0,1  4,1  2,1  3,1
            p[5] = bm.createPage(); // error
        } catch (Exception e) {
            // no unpinned pages
            foundError[0] = true;
        }
        try {
            bm.getPage(p[0].getId());
            bm.unpinPage(p[0].getId());
            // 0,1  4,1  2,1  3,1
            p[5] = bm.createPage(); // error
        } catch (Exception e) {
            // no unpinned pages (pins on p[0] went from 1 to 2 to 1, still not
            // zero even though unpinPage was called)
            foundError[1] = true;
        }
        try {
            bm.unpinPage(p[0].getId());
            // 0,0  4,1  2,1  3,1
            p[5] = bm.createPage();
            bm.unpinPage(p[4].getId());
            bm.unpinPage(p[2].getId());
            bm.unpinPage(p[3].getId());
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed due to unexpected error: " + e.toString();
        }
        if (p[5].getId() != 5) {
            return "Failed due to page ID mismatch. Expected 5, got " + p[5].getId();
        }
        try {
            // 5,1  4,0  2,0  3,0
            bm.unpinPage(p[1].getId()); // error
        } catch (Exception e) {
            // p[1] not in buffer
            foundError[2] = true;
        }
        return "Passed lruLongerTest(), and expected " + foundError.length + " exceptions to be thrown. Caught status " + Arrays.toString(foundError);
    }

}
