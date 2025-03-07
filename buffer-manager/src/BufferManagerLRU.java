import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

// Buffer manager that uses a Least Recently Used (LRU) policy for page replacement
// Buffer manager maintains a buffer pool of pages in memory
// Buffer manager consists of frames, each frame holds a page
class BufferManagerLRU extends BufferManager {
    private int bufferSize; // Number of frames in buffer pool
    private Page[] bufferPool; // Array of pages in memory, indexed by frame index
    private boolean[] isDirty; // Dirty bit for each page/frame
    private int[] isPinned; // flag to indicate if page is pinned, flag since implementation is single threaded
    private Map<Integer, Integer> pageTable; // pageId : frame index
    private LinkedList<Integer> lruQueue; // Queue of unpinned pageIds in LRU order
    private int nextPageId;
    private int usedFrames;
    private boolean printStuff;

    public BufferManagerLRU(int bufferSize, boolean printStuff) {
        super(bufferSize);
        this.bufferSize = bufferSize;
        this.printStuff = printStuff;
        this.bufferPool = new Page[bufferSize];
        this.isDirty = new boolean[bufferSize];
        this.isPinned = new int[bufferSize];
        this.pageTable = new HashMap<>();
        this.lruQueue = new LinkedList<>();
        initFile();
        this.nextPageId = 0;
        this.usedFrames = 0;
    }

    public BufferManagerLRU(int bufferSize) {
        // printStuff is false by defeault, just nice for testing.
        this(bufferSize, false);
    }

    @Override
    Page createPage() {
        // If buffer is full and all pages are pinned, can't create new page
        if (usedFrames >= bufferSize && !isPageEvicted()) {
            System.err.println("Error: Cannot create a new page. Buffer is full, all pages are pinned.");
            return null;
        }

        Page page = new PageImpl(nextPageId++); // increment nextPageId post page creation
        int frameIdx = addPageToBufferPool(page);
        isPinned[frameIdx] = 1;
        markDirty(page.getId());

        return page;
    }

    @Override
    Page getPage(int pageId) {
        Page page = null;

        // Check if pageId is valid
        if (pageId >= nextPageId){
            System.out.println("Invalid page id, requested id: " + pageId + " highest id is: " + (nextPageId-1));
            return page;
        } 

        // Check if page is already in buffer pool
        // If found, increment pin count and return page, make it lru
        if (pageTable.containsKey(pageId)) {
            int frameIdx = pageTable.get(pageId);
            isPinned[frameIdx] = 1;
            lruQueue.remove((Integer) pageId); // Remove from LRU

            if(printStuff){
                System.out.println("Cache hit: Page " + pageId);
            }

            return bufferPool[frameIdx];
        }

        if (usedFrames >= bufferSize && !isPageEvicted()) {
            System.out.println("Error: Cannot load page " + pageId + ". Buffer is full, all pages are pinned.");
            return null;
        }

        page = readPageFromDisk(pageId);
    
        if (page != null) {
            if(printStuff){
                System.out.println("Cache miss: Loaded Page " + pageId + " from disk");
            }

            int frameIdx = addPageToBufferPool(page);
            isPinned[frameIdx] = 1; // Pinned on load
        }

        return page;
    }

    @Override
    public void unpinPage(int pageId) {
        if (pageTable.containsKey(pageId)) {
            int frameIdx = pageTable.get(pageId);

            if (isPinned[frameIdx] > 0) {
                isPinned[frameIdx] = 0;

                if(printStuff){
                    System.out.println("Unpinned page " + pageId);
                }

                if (isPinned[frameIdx] == 0) {
                    lruQueue.add(pageId); // Add to LRU queue when unpinned
                }
            }
        }
    }

    @Override
    public void markDirty(int pageId) {
        if (pageTable.containsKey(pageId)) {
            int frameIdx = pageTable.get(pageId);
            isDirty[frameIdx] = true;
            if(printStuff){
                System.out.println("Marked page " + pageId + " as dirty");
            }
        }
    }

    // Adds a page to buffer pool, increments usedFrames
    // @return frame index of added page
    private int addPageToBufferPool(Page page) {
        int frameIdx = usedFrames++;
        bufferPool[frameIdx] = page;
        pageTable.put(page.getId(), frameIdx);

        if(printStuff){
            System.out.println("Added page " + page.getId() + " to frame " + frameIdx);
        }
        
        return frameIdx;
    }

    // Evicts the least recently used page from buffer pool
    // @return true if a page was evicted, false otherwise
    private boolean isPageEvicted() {
        if (lruQueue.isEmpty()) {
            return false; // No unpinned pages to evict
        }

        int pageId = lruQueue.poll(); // Get least recently used unpinned page
        int frameIdx = pageTable.get(pageId); // Get frame index of page
        Page page = bufferPool[frameIdx];

        if(printStuff){
            System.out.println("Evicting page " + pageId + " from frame " + frameIdx);
        }

        // Check if page is dirty, write to disk if dirty
        if (isDirty[frameIdx]) {
            if(printStuff){
                System.out.println("Dirty page " + pageId);
            }

            if (!writePageToDisk(page)) {
                System.out.println("Error: Failed to write dirty page " + pageId + " to disk");
                return false; // Failed to write dirty page to disk
            }

            isDirty[frameIdx] = false;
            if(printStuff){
                System.out.println("Wrote page " + pageId + " to disk, marked undirty");
            }
        }

        // Swap with last frame instead of shifting
        int lastFrameIdx = usedFrames - 1;

        // Update pageTable, bufferPool, isDirty if evicted page is not the last page
        if (frameIdx != lastFrameIdx) {
            bufferPool[frameIdx] = bufferPool[lastFrameIdx];
            isDirty[frameIdx] = isDirty[lastFrameIdx];
            isPinned[frameIdx] = isPinned[lastFrameIdx];
            pageTable.put(bufferPool[frameIdx].getId(), frameIdx);
        }

        bufferPool[lastFrameIdx] = null;
        isDirty[lastFrameIdx] = false;
        isPinned[lastFrameIdx] = 0;
        pageTable.remove(pageId);
        usedFrames--;
        //System.out.println(pageTable);
        if(printStuff){
            System.out.println("Evicted page " + pageId + " from frame " + frameIdx);
            System.out.println("Moved page " + bufferPool[frameIdx].getId() + " from frame " + lastFrameIdx + " to frame " + frameIdx);
        }
        return true;
    }

    private void initFile() {
        File file = new File(Constants.BINARY_FILE_PATH);

        if (!file.exists()) {
            try {
                if (file.createNewFile()) {
                    System.out.println("Created new file: " + Constants.BINARY_FILE_PATH);
                } else {
                    System.out.println("Failed to create file: " + Constants.BINARY_FILE_PATH);
                }
            } catch (IOException e) {
                System.err.println("Error creating file " + Constants.BINARY_FILE_PATH + ": " + e.getMessage());
            }
        }
    }

    // Reads bytes from disk
    // todo: implement page.deserialize(data) and test
    private Page readPageFromDisk(int pageId) {
        if (pageId >= nextPageId) {
            return null; // no such page in disk
        }

        try (RandomAccessFile raf = new RandomAccessFile(Constants.BINARY_FILE_PATH, "r")) {
            raf.seek((long) pageId * Constants.PAGE_SIZE);
            byte[] data = new byte[Constants.PAGE_SIZE];
            raf.readFully(data);

            Page pageFromDisk = new PageImpl(pageId);

            boolean success = pageFromDisk.deserialize(data); 
            if(!success){
                System.out.println("Something went wrong in deserialization, there was not enough space.");
            }
            return pageFromDisk;
        } catch (FileNotFoundException ex) {
            System.err.println("Could not find binary file.");
            ex.printStackTrace();
        } catch (IOException ex) {
            System.err.println("Exception while reading from disk");
            ex.printStackTrace();
        }

        return null;
    }

    // Writes bytes to disk
    // @return true if successful
    // caller to mark the page dirty and update lru
    // TODO: page.serialize() should return byte[], implement page.serialize(), test using loaddatabase
    private boolean writePageToDisk(Page page) {
        try (RandomAccessFile raf = new RandomAccessFile(Constants.BINARY_FILE_PATH, "rw")) {
            raf.seek((long) page.getId() * Constants.PAGE_SIZE);
            raf.write(page.serialize()); // TODO page.serialize() should return byte[]

            return true;
        } catch (FileNotFoundException ex) {
            System.err.println("Could not create binary file.");
            ex.printStackTrace();
        } catch (IOException ex) {
            System.err.println("Exception while reading from disk");
            ex.printStackTrace();
        }

        return false;
    }
}