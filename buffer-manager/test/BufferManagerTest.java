import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Comments are intended to give an idea of what the buffer might look like, but
 * the real contents could differ depending on implementation. Adapted to work with JUnit.
 */
public class BufferManagerTest {

    public static BufferManager getNewBM(int bufferSize) {
        // change to whichever BufferManager extension you want to test
        return new LRUBufferManager(bufferSize);
    }

    public static void main(String[] args) {

        // test cases, each has no arguments and returns a string result
        @SuppressWarnings("unchecked")
        Supplier<String>[] tests = new Supplier[] {
                BufferManagerTest::overfillBuffer,
                BufferManagerTest::evictPage,
                BufferManagerTest::lruLongerTest
        };

        for (int i = 0; i < tests.length; ++i) {
            System.out.println("TEST " + (i + 1) + " RESULT: " + tests[i].get());
        }

    }

    public static String overfillBuffer() {
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
        } catch (IOException e) {
            return "IO error: " + e.toString();
        } catch (Exception e) {
            caughtError = true;
        }
        return "Passed overfillBuffer(), and expected 1 exceptions to be thrown. Caught status [" + caughtError + "]";
    }

    public static String evictPage() {
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
        } catch (IOException e) {
            return "IO error: " + e.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed due to unexpected error: " + e.toString();
        }
        return "Passed evictPage(), and expected no exceptions to be thrown.";
    }

    public static String lruLongerTest() {
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
        } catch (IOException e) {
            return "IO error: " + e.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed due to unexpected error: " + e.toString();
        }
        boolean[] foundError = new boolean[3]; // update number of errors here
        try {
            // 0,1  4,1  2,1  3,1
            p[5] = bm.createPage(); // error
        } catch (IOException e) {
            return "IO error: " + e.toString();
        } catch (Exception e) {
            // no unpinned pages
            foundError[0] = true;
        }
        try {
            bm.getPage(p[0].getId());
            bm.unpinPage(p[0].getId());
            // 0,1  4,1  2,1  3,1
            p[5] = bm.createPage(); // error
        } catch (IOException e) {
            return "IO error: " + e.toString();
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
        } catch (IOException e) {
            return "IO error: " + e.toString();
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
