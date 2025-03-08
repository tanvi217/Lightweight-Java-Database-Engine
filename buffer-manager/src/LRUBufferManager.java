import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Iterator;

public class LRUBufferManager extends BufferManager {

    private int pageBytes;
    private ByteBuffer buffer;
    private String binFile;
    private LinkedHashMap<Integer, Integer> pageTable;
    private Page[] bufferPages;
    private boolean[] isDirty;
    private int[] pinCount;
    private int nextPageId;

    /**
     * The LinkedHashMap pageTable is initialized with parameters (initialCapacity,
     * loadFactor, accessOrder). The capacity and load factor are set such that the
     * hash map should not have to perform an internal resize at any point. The
     * access order flag is 'false' so that pageTable.keySet().iterator() will
     * iterate over the page IDs in the order of least recently inserted. The loop
     * in this constructor maps the page IDs {-1, -2, ..., -bufferSize} to a unique
     * frame index. Because the default values for Page, boolean, and int are null,
     * false, and 0, these placeholder Pages are null, not dirty, and unpinned.
     * 
     * @param numFrames The number of frames, passed into the BufferManager
     *                  constructor. Same as bufferSize.
     * @param pageKB Number of kibibytes in a page.
     * @param binaryFileName Relative path to binary file.
     */
    public LRUBufferManager(int numFrames, int pageKB, String binaryFileName) {
        super(numFrames);
        pageBytes = pageKB * 1024;
        buffer = ByteBuffer.allocate(bufferSize * pageBytes);
        binFile = binaryFileName;
        pageTable = new LinkedHashMap<>(1 + (bufferSize * 4) / 3, 0.75f, false);
        for (int i = 1; i <= bufferSize; ++i) {
            pageTable.put(-i, i - 1);
        }
        bufferPages = new Page[bufferSize];
        isDirty = new boolean[bufferSize];
        pinCount = new int[bufferSize];
        nextPageId = 0;
        initFile();
    }

    /**
     * For use with IMDb data. Select which Page implementation to use here.
     */
    public LRUBufferManager(int numFrames) {
        this(numFrames, 4, Constants.DATA_BIN_FILE);
    }

    /**
     * Creates the binary file if it doesn’t exist yet.
     */
    private void initFile() {
        File file = new File(binFile);
        if (file.exists()) {
            return;
        }
        try {
            if (file.createNewFile()) {
                System.out.println("Created new file: " + binFile);
            } else {
                System.out.println("Failed to create file: " + binFile);
            }
        } catch (IOException e) {
            System.err.println("Error creating file " + binFile + ": " + e.getMessage());
        }
    }

    private Page getNewPage(int pageId, int frameIndex) {
        return new IMDbPage(pageId, frameIndex, pageBytes, buffer); // could change Page implementation here
    }

    // Reads bytes from disk
    private Page readPageFromDisk(int pageId, int frameIndex) throws IOException {
        if (pageId < 0 || pageId > nextPageId) { // pageId = nextPageId during createPage
            throw new IllegalArgumentException("Page ID out of bounds.");
        }
        try (RandomAccessFile raf = new RandomAccessFile(binFile, "r")) {
            raf.seek((long) pageId * pageBytes);
            byte[] data = new byte[pageBytes];
            raf.readFully(data);
            return getNewPage(pageId, frameIndex);
        } catch (FileNotFoundException ex) {
            System.err.println("Could not find binary file.");
            throw ex;
        } catch (IOException ex) {
            System.err.println("Exception while reading from disk");
            throw ex;
        }
    }

    // Writes bytes to disk
    // @return true if successful
    // caller to mark the page dirty and update lru
    private boolean writePageToDisk(Page page) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(binFile, "rw")) {
            int frameIndex = pageTable.get(page.getId());
            int pageStart = frameIndex * pageBytes;
            raf.seek(pageStart);
            raf.write(buffer.array(), pageStart, pageBytes);
            return true;
        } catch (FileNotFoundException ex) {
            System.err.println("Could not create binary file.");
            throw ex;
        } catch (IOException ex) {
            System.err.println("Exception while reading from disk");
            throw ex;
        }
    }

    /**
     * Replaces page in bufferPool at frameIndex, writing to disk if necessary.
     * Ignores pins, and resets isDirty and pinCount at frameIndex.
     */
    private void overwriteFrame(int prevPageId, Page nextPage) throws IOException {
        int frameIndex = pageTable.get(prevPageId);
        Page prevPage = bufferPages[frameIndex];
        if (isDirty[frameIndex] && prevPage != null) {
            writePageToDisk(prevPage);
        }
        pageTable.remove(prevPageId);
        bufferPages[frameIndex] = nextPage;
        isDirty[frameIndex] = false;
        pinCount[frameIndex] = 0;
        pageTable.put(nextPage.getId(), frameIndex);
    }

    /**
     * Doesn't change any instance variables, and returns the pageId of the
     * least recently used unpinned page currently in bufferPool.
     */
    private int leastRecentlyUsedPage() {
        Iterator<Integer> lruPageIds = pageTable.keySet().iterator();
        while (lruPageIds.hasNext()) {
            int pageId = lruPageIds.next();
            int frameIndex = pageTable.get(pageId);
            if (pinCount[frameIndex] == 0) {
                return pageId;
            }
        }
        throw new IllegalStateException("Buffer contains no unpinned pages.");
    }

    @Override
    public Page getPage(int pageId) throws IOException {
        if (pageId < 0) {
            throw new IllegalArgumentException("No page can have ID less than zero.");
        }
        int frameIndex;
        if (pageTable.containsKey(pageId)) {
            frameIndex = pageTable.get(pageId);
            pageTable.remove(pageId); // remove so that insertion resets pageId's position in pageTable.keySet()
            pageTable.put(pageId, frameIndex);
        } else {
            int lruPageId = leastRecentlyUsedPage();
            frameIndex = pageTable.get(lruPageId);
            Page nextPage = readPageFromDisk(pageId, frameIndex);
            overwriteFrame(lruPageId, nextPage);
        }
        pinCount[frameIndex] += 1;
        return bufferPages[frameIndex];
    }

    @Override
    public Page createPage() throws IOException {
        Page pageObject = getPage(nextPageId); // inserts nextPageId into pageTable
        int frameIndex = pageTable.get(nextPageId);
        nextPageId += 1;
        isDirty[frameIndex] = true;
        return pageObject;
    }

    @Override
    public void markDirty(int pageId) {
        if (pageId < 0 || !pageTable.containsKey(pageId)) {
            throw new IllegalArgumentException("No page with this ID is in the buffer.");
        }
        int frameIndex = pageTable.get(pageId);
        isDirty[frameIndex] = true;
    }

    @Override
    public void unpinPage(int pageId) {
        if (pageId < 0 || !pageTable.containsKey(pageId)) {
            throw new IllegalArgumentException("No page with this ID is in the buffer.");
        }
        int frameIndex = pageTable.get(pageId);
        if (pinCount[frameIndex] == 0) {
            throw new IllegalStateException("Cannot unpin page with no pins.");
        }
        pinCount[frameIndex] -= 1;
    }

    @Override
    public String toString() {
        int d = String.valueOf(nextPageId - 1).length();
        int[] numDigits = { 2, 2, 2, 3, 5, 5, 7, 7 };
        int[] numColumns = { 8, 8, 8, 6, 4, 4, 3, 3 };
        int rowSize = d > 7 ? 2 : numColumns[d];
        int idSize = d > 7 ? 11 : numDigits[d];
        StringBuilder sb = new StringBuilder("max pageId " + (nextPageId - 1) + "\n");
        int i = 0;
        while (i < bufferSize) {
            for (int j = 0; j < rowSize && i < bufferSize; ++j) {
                Page p = bufferPages[i];
                sb.append(" ");
                if (p != null) {
                    sb.append(String.format("%0" + idSize + "d", p.getId()));
                } else {
                    sb.append("-".repeat(idSize));
                }
                ++i;
            }
            sb.append("\n");
        }
        return sb.toString();
    }

}
