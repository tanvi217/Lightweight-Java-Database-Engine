import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

// Buffer manager that uses a Least Recently Used (LRU) policy for page replacement
// Buffer manager maintains a buffer pool of pages in memory
// Buffer manager consists of frames, each frame holds a page
public class LRUBufferManager extends BufferManager {

    // abstract class BufferManager stores 'int bufferSize', the number of frames in buffer pool
    private int bytesInPage;
    private ByteBuffer buffer;
    private LinkedHashMap<Integer, Integer> pageTable; // pageKey : frame index
    private Page[] bufferPages; // Array of pages in memory, indexed by frame index
    private String[] fileToWrite; // Stores the name of the file to write to. Name 'imdb' is used to refer to the file '../data/imdb.bin'.
    private boolean[] isDirty; // Dirty bit for each frame
    private int[] pinCount; // Pin count for each frame
    private Map<String, Integer> numPagesInFile; // changed to an map so we can keep track for the different files
    private Map<String, Integer> fileIds;
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
     * @param numPagesInBuffer The number of frames, passed into the BufferManager
     *                         constructor. Same as bufferSize.
     * @param pageKB           Number of kibibytes in a page.
     * @param debugPrinting    True if additional print statements should be run.
     */
    public LRUBufferManager(int numPagesInBuffer, int pageKB, boolean debugPrinting) {
        super(numPagesInBuffer);
        bytesInPage = pageKB * 1024;
        buffer = ByteBuffer.allocate(bufferSize * bytesInPage);
        pageTable = new LinkedHashMap<>(1 + (bufferSize * 4) / 3, 0.75f, false);
        
        for (int i = 1; i <= bufferSize; ++i) {
            pageTable.put(-i, i - 1);
        }

        bufferPages = new Page[bufferSize];
        fileToWrite = new String[bufferSize];
        isDirty = new boolean[bufferSize];
        pinCount = new int[bufferSize];
        numPagesInFile = new HashMap<>();
        fileIds = new HashMap<>();
        this.debugPrinting = debugPrinting;
    }

    /*
     * Constructor overloading to allow caller to use default values.
     */
    public LRUBufferManager(int numPagesInBuffer, boolean debugPrinting) { this(numPagesInBuffer, Constants.PAGE_KB, debugPrinting); }
    public LRUBufferManager(int numPagesInBuffer) { this(numPagesInBuffer, Constants.PAGE_KB, false); }
    public LRUBufferManager(boolean debugPrinting) { this(Constants.BUFFER_SIZE, Constants.PAGE_KB, debugPrinting); }
    public LRUBufferManager() { this(Constants.BUFFER_SIZE, Constants.PAGE_KB, false); }

    private String pathToFile(String filename) {
        return Constants.DATA_DIRECTORY + filename + ".bin";
    }

    /**
     * The function createNewFile() creates a file from the File object, unless it
     * exists already. If 'filename.bin' is in the data directory already, then
     * nothing changes.
     * 
     * @param filename The name of the file to be initialized.
     */
    private void initFile(String filename) {
        if (filename.equals("__temp__")) return;  // Skip in-memory temp files
    
        try {
            File file = new File(pathToFile(filename));
            File parent = file.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();  // Creates "data/" directory if missing
            }
    
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            System.out.println("I/O Error: the data directory may be misplaced.");
            e.printStackTrace();
        }
    }
    

    private int getNextPageId(String filename) {
        if (!numPagesInFile.containsKey(filename)) {
            initFile(filename);
            numPagesInFile.put(filename, 0);
            fileIds.put(filename, fileIds.size());
        }
        return numPagesInFile.get(filename);
    }

    /**
     * Creates a new Page. The implementation could be changed to use a different Page class.
     */
    private Page getPageObject(int pageId, int frameIndex, int rowLength) {
        int pageStart = frameIndex * bytesInPage;
        return new TabularPage(pageId, pageStart, bytesInPage, buffer, rowLength);
    }

    /**
     * Takes a page ID of a page which has already been created, and a reads that page from the disk into a given frame.
     */
    private void readPageFromDisk(int pageId, int frameIndex) {
        if (pageId < 0 || pageId >= getNextPageId(fileToWrite[frameIndex])) {
            throw new IllegalArgumentException("Internal Read Error: Page ID out of bounds");
        }

        try (RandomAccessFile raf = new RandomAccessFile(pathToFile(fileToWrite[frameIndex]), "r")) {
            raf.seek(pageId * bytesInPage); // move to location of page on disk
            raf.read(buffer.array(), frameIndex * bytesInPage, bytesInPage); // read a full page of data into the buffer, starting at the location of the frame
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // one to one mapping between pageKeys and (pageId, fileId) pairs
    private int getPageKey(int pageId, String filename) {
        if (!fileIds.containsKey(filename)) {
            throw new IllegalArgumentException("File with this name has not been seen before.");
        }
        int fileId = fileIds.get(filename);
        return (2 * pageId + 1) * (1 << fileId) - 1;
    }

    /**
     * Given page ID of a page in the buffer, finds the correct frame and writes the contents of that frame to the disk at the location of the page.
     */
    private void writePageToDisk(int pageId, int pageKey) {
        if (pageId < 0 || !pageTable.containsKey(pageKey)) {
            throw new IllegalArgumentException("Internal Write Error: Page with this ID and file is not in the buffer");
        }
        int frameIndex = pageTable.get(pageKey);
        try (RandomAccessFile raf = new RandomAccessFile(pathToFile(fileToWrite[frameIndex]), "rw")) {
            raf.seek(pageId * bytesInPage); // move to location of page on disk
            raf.write(buffer.array(), frameIndex * bytesInPage, bytesInPage);  // write a full page of data into the buffer, starting at the location of the frame
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Changes instance variables to be consistent with new page.
     */
    private void overwritePage(int oldPageKey, int newPageId, String newPageFilename, int rowLength) {
        int frameIndex = pageTable.get(oldPageKey);

        if (isDirty[frameIndex]) {
            int oldPageId = bufferPages[frameIndex].getId();
            writePageToDisk(oldPageId, oldPageKey);
            if (debugPrinting) {
                System.err.println("Wrote page " + oldPageId + " to disk");
            }
        }

        boolean newPageIsEmpty = rowLength != 0;
        if (!newPageIsEmpty) {
            readPageFromDisk(newPageId, frameIndex);
        }

        pageTable.remove(oldPageKey);
        int newPageKey = getPageKey(newPageId, newPageFilename);
        pageTable.put(newPageKey, frameIndex);
        bufferPages[frameIndex] = getPageObject(newPageId, frameIndex, rowLength);
        fileToWrite[frameIndex] = newPageFilename;
        isDirty[frameIndex] = newPageIsEmpty; // newly created page is marked dirty
        pinCount[frameIndex] = 1;

        if (debugPrinting) {
            System.out.println("Added page " + newPageId + " to frame " + frameIndex);
        }
    }

    /**
     * Doesn't change any instance variables, and returns the pageKey of the
     * least recently used unpinned page currently in bufferPages.
     */
    private int pageKeyOfLRUPage() {
        Iterator<Integer> lruPageKeys = pageTable.keySet().iterator();

        while (lruPageKeys.hasNext()) {
            int pageKey = lruPageKeys.next();
            int frameIndex = pageTable.get(pageKey);

            if (pinCount[frameIndex] == 0) {
                if (debugPrinting) {
                    System.out.println("Found least recently used page with key " + pageKey);
                }

                return pageKey;
            }
        }
        throw new IllegalStateException("Buffer contains no unpinned pages.");
    }

    @Override
    public Page getPage(int pageId, String filename) {
        if (pageId < 0 || pageId >= getNextPageId(filename)) {
            System.out.println("Invalid page id, requested id: " + pageId + " highest id is: " + (getNextPageId(filename) - 1));
            throw new IllegalArgumentException("Error Getting Page " + pageId + ": ID is out of bounds");
        }

        int pageKey = getPageKey(pageId, filename);
        if (pageTable.containsKey(pageKey)) {
            int frameIndex = pageTable.get(pageKey);
            pageTable.remove(pageKey); // remove so that insertion resets pageId's position in pageTable.keySet()
            pageTable.put(pageKey, frameIndex);
            pinCount[frameIndex] += 1;

            if (debugPrinting) {
                System.out.println("Cache hit: Page " + pageId);
            }

            return bufferPages[frameIndex];
        }

        int lruPageKey = pageKeyOfLRUPage();
        int frameIndex = pageTable.get(lruPageKey);
        overwritePage(lruPageKey, pageId, filename, 0); // sets pin count to 1

        if (debugPrinting) {
            System.out.println("Cache miss: Loaded Page " + pageId + " from disk");
        }

        return bufferPages[frameIndex];
    }

    @Override
    public Page createPage(String filename, int rowLength) {
        int lruPageKey = pageKeyOfLRUPage();
        int frameIndex = pageTable.get(lruPageKey);

        int nextPageId = getNextPageId(filename);
        overwritePage(lruPageKey, nextPageId, filename, rowLength); // marks new page dirty, and sets pin count to 1
        numPagesInFile.put(filename, nextPageId + 1);

        return bufferPages[frameIndex];
    }

    @Override
    public void markDirty(int pageId, String filename) {
        int pageKey = getPageKey(pageId, filename);
        if (pageId < 0 || !pageTable.containsKey(pageKey)) {
            throw new IllegalArgumentException("Error Marking Page " + pageId + " Dirty: No page with this ID is in the buffer.");
        }

        int frameIndex = pageTable.get(pageKey);
        isDirty[frameIndex] = true;

        if (debugPrinting) {
            System.out.println("Marked page " + pageId + " as dirty");
        }
    }

    @Override
    public void markClean(int pageId, String filename) {
        int pageKey = getPageKey(pageId, filename);
        if (pageId < 0 || !pageTable.containsKey(pageKey)) {
            throw new IllegalArgumentException("Error Marking Page " + pageId + " Clean: No page with this ID is in the buffer.");
        }
    
        int frameIndex = pageTable.get(pageKey);
        isDirty[frameIndex] = false;
    
        if (debugPrinting) {
            System.out.println("Marked page " + pageId + " as clean");
        }
    }
    

    @Override
    public void unpinPage(int pageId, String filename) {
        int pageKey = getPageKey(pageId, filename);
        if (pageId < 0 || !pageTable.containsKey(pageKey)) {
            throw new IllegalArgumentException("Error Unpinning Page " + pageId + ": No page with this ID is in the buffer.");
        }

        int frameIndex = pageTable.get(pageKey);

        if (pinCount[frameIndex] == 0) {
            throw new IllegalStateException("Error Unpinning Page " + pageId + ": Cannot unpin page with no pins.");
        }

        pinCount[frameIndex] -= 1;
    }

    @Override
    public String toString() {
        int totalNumPages = 0;
        for (int num : numPagesInFile.values()) {
            totalNumPages += num;
        }
        //tried to change this to work with the new nextPageId array but not entirely sure it works
        int d = String.valueOf(totalNumPages - 3).length();
        int[] numDigits = { 2, 2, 2, 3, 5, 5, 7, 7 };
        int[] numColumns = { 8, 8, 8, 6, 4, 4, 3, 3 };
        int rowSize = d > 7 ? 2 : numColumns[d];
        int idSize = d > 7 ? 11 : numDigits[d];
        String info = String.format("BUFFER  pages: %d  frames: %d  full-length: %d bytes", totalNumPages, pageTable.keySet().size(), buffer.capacity());
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

    @Override
    public void force() {
        for (int i = 0; i < bufferSize; ++i) {
            if (isDirty[i]) {
                int pageId = bufferPages[i].getId();
                int pageKey = getPageKey(pageId, fileToWrite[i]);
                try {
                    writePageToDisk(pageId, pageKey);
                } catch (Exception e) {}
            }
            isDirty[i] = false;
            pinCount[i] = 0;
        }
    }

}
