import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Iterator;

// Buffer manager that uses a Least Recently Used (LRU) policy for page replacement
// Buffer manager maintains a buffer pool of pages in memory
// Buffer manager consists of frames, each frame holds a page
public class LRUBufferManager extends BufferManager {

    // abstract class BufferManager stores 'int bufferSize', the number of frames in buffer pool
    private int pageBytes;
    private ByteBuffer buffer;
    private String binFile;
    private LinkedHashMap<Integer, Integer> pageTable; // pageId : frame index
    private Page[] bufferPages; // Array of pages in memory, indexed by frame index
    private boolean[] isDirty; // Dirty bit for each frame
    private int[] pinCount; // Pin count for each frame
    private int nextPageId;
    private boolean debugPrinting;

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
    public LRUBufferManager(int numFrames, int pageKB, String binaryFileName, boolean debugPrinting) {
        super(numFrames);
        this.debugPrinting = debugPrinting;
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
    public LRUBufferManager(int numFrames, boolean debugPrinting) {
        this(numFrames, 4, Constants.DATA_BIN_FILE, debugPrinting);
    }

    public LRUBufferManager(int numFrames) {
        this(numFrames, 4, Constants.DATA_BIN_FILE, false);
    }

    /**
     * Creates the binary file if it doesn’t exist yet.
     */
    private void initFile() {
        try {
            new File(binFile).createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a new Page. The implementation could be changed to use a different Page class.
     */
    private Page getPageObject(int pageId, int frameIndex, boolean isEmpty) {
        return new IMDbPage(pageId, frameIndex, pageBytes, buffer, isEmpty);
    }

    /**
     * Takes a page ID of a page which has already been created, and a reads that page from the disk into a given frame.
     */
    private void readPageFromDisk(int pageId, int frameIndex) {
        if (pageId < 0 || pageId >= nextPageId) {
            throw new IllegalArgumentException("Internal Read Error: Page ID out of bounds");
        }

        try (RandomAccessFile raf = new RandomAccessFile(binFile, "r")) {
            raf.seek(pageId * pageBytes); // move to location of page on disk
            raf.read(buffer.array(), frameIndex * pageBytes, pageBytes); // read a full page of data into the buffer, starting at the location of the frame
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Given page ID of a page in the buffer, finds the correct frame and writes the contents of that frame to the disk at the location of the page.
     */
    private void writePageToDisk(int pageId) {
        if (pageId < 0 || !pageTable.containsKey(pageId)) {
            throw new IllegalArgumentException("Internal Write Error: Page with this ID is not in the buffer");
        }

        try (RandomAccessFile raf = new RandomAccessFile(binFile, "rw")) {
            int frameIndex = pageTable.get(pageId);

            raf.seek(pageId * pageBytes); // move to location of page on disk
            raf.write(buffer.array(), frameIndex * pageBytes, pageBytes);  // write a full page of data into the buffer, starting at the location of the frame
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Changes instance variables to be consistent with new page.
     */
    private void overwritePage(int oldPageId, int newPageId, boolean newPageIsEmpty) {
        int frameIndex = pageTable.get(oldPageId);

        if (isDirty[frameIndex]) {
            writePageToDisk(oldPageId);
            if (debugPrinting) {
                System.err.println("Wrote page " + oldPageId + " to disk");
            }
        }

        if (!newPageIsEmpty) {
            readPageFromDisk(newPageId, frameIndex);
        }

        pageTable.remove(oldPageId);
        pageTable.put(newPageId, frameIndex);
        bufferPages[frameIndex] = getPageObject(newPageId, frameIndex, newPageIsEmpty);
        isDirty[frameIndex] = newPageIsEmpty; // newly created page is marked dirty
        pinCount[frameIndex] = 1;

        if (debugPrinting) {
            System.out.println("Added page " + newPageId + " to frame " + frameIndex);
        }
    }

    /**
     * Doesn't change any instance variables, and returns the pageId of the
     * least recently used unpinned page currently in bufferPages.
     */
    private int pageIdOfLRUPage() {
        Iterator<Integer> lruPageIds = pageTable.keySet().iterator();

        while (lruPageIds.hasNext()) {
            int pageId = lruPageIds.next();
            int frameIndex = pageTable.get(pageId);

            if (pinCount[frameIndex] == 0) {
                if (debugPrinting) {
                    System.out.println("Found least recently used page " + pageId);
                }

                return pageId;
            }
        }
        throw new IllegalStateException("Buffer contains no unpinned pages.");
    }

    @Override
    public Page getPage(int pageId) {
        if (pageId < 0 || pageId >= nextPageId) {
            System.out.println("Invalid page id, requested id: " + pageId + " highest id is: " + (nextPageId - 1));
            throw new IllegalArgumentException("Error Getting Page " + pageId + ": ID is out of bounds");
        }

        if (pageTable.containsKey(pageId)) {
            int frameIndex = pageTable.get(pageId);
            pageTable.remove(pageId); // remove so that insertion resets pageId's position in pageTable.keySet()
            pageTable.put(pageId, frameIndex);
            pinCount[frameIndex] += 1;

            if (debugPrinting) {
                System.out.println("Cache hit: Page " + pageId);
            }

            return bufferPages[frameIndex];
        }

        int lruPageId = pageIdOfLRUPage();
        int frameIndex = pageTable.get(lruPageId);
        overwritePage(lruPageId, pageId, false); // sets pin count to 1

        if (debugPrinting) {
            System.out.println("Cache miss: Loaded Page " + pageId + " from disk");
        }

        return bufferPages[frameIndex];
    }

    @Override
    public Page createPage() {
        int lruPageId = pageIdOfLRUPage();
        int frameIndex = pageTable.get(lruPageId);

        overwritePage(lruPageId, nextPageId, true); // marks new page dirty, and sets pin count to 1
        nextPageId += 1;

        return bufferPages[frameIndex];
    }

    @Override
    public void markDirty(int pageId) {
        if (pageId < 0 || !pageTable.containsKey(pageId)) {
            throw new IllegalArgumentException("Error Marking Page " + pageId + " Dirty: No page with this ID is in the buffer.");
        }

        int frameIndex = pageTable.get(pageId);
        isDirty[frameIndex] = true;

        if (debugPrinting) {
            System.out.println("Marked page " + pageId + " as dirty");
        }
    }

    @Override
    public void unpinPage(int pageId) {
        if (pageId < 0 || !pageTable.containsKey(pageId)) {
            throw new IllegalArgumentException("Error Unpinning Page " + pageId + ": No page with this ID is in the buffer.");
        }

        int frameIndex = pageTable.get(pageId);

        if (pinCount[frameIndex] == 0) {
            throw new IllegalStateException("Error Unpinning Page " + pageId + ": Cannot unpin page with no pins.");
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
        String info = String.format("BUFFER  pages: %d  frames: %d  full-length: %d bytes", nextPageId, pageTable.keySet().size(), buffer.capacity());
        StringBuilder sb = new StringBuilder(info);
        int i = 0;

        while (i < bufferSize) {
            sb.append("\n");

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
        }

        return sb.toString();
    }

}
