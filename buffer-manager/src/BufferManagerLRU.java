import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

// Buffer manager that uses a Least Recently Used (LRU) policy for page replacement
// Buffer manager maintains a buffer pool of pages in memory
// Buffer manager consists of frames, each frame holds a page
class BufferManagerLRU extends BufferManager {
    private int bufferSize; // Number of frames in buffer pool
    private Page[] bufferPool; // Array of pages in memory, indexed by frame index
    private boolean[] isDirty; // Dirty bit for each frame
    private int[] isPinned; // Pin count for each frame
    private Map<Integer, Integer> pageTable; // pageId : frame index
    private LinkedHashMap<Integer, Boolean> lruQueue; // Queue of pageIds in LRU order
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
        this.lruQueue = new LinkedHashMap<Integer, Boolean>();
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
        // throw an error
        if (usedFrames >= bufferSize && !isPageEvicted()) {
            System.err.println("Error: Cannot create a new page. Buffer is full, all pages are pinned.");
            throw new IllegalStateException("Buffer is full, all pages are pinned, cannot create new page.");
        }

        Page page = new BinaryPage(nextPageId++); // increment nextPageId post page creation
        int frameIdx = addPageToBufferPool(page);
        isPinned[frameIdx] = 1;
        lruQueue.put(page.getId(), false); // Add to LRU queue when created

        // check: do we need to mark it dirty yet?
        // caller does mark it dirty when they write to it
        // not dirty yet, but mark it dirty so it gets written to disk when evicted
        markDirty(page.getId());

        return page;
    }

    @Override
    Page getPage(int pageId) {
        Page page = null;

        // Check if pageId is valid
        if (pageId >= nextPageId){
            System.out.println("Invalid page id, requested id: " + pageId + " highest id is: " + (nextPageId - 1));

            return null;
        } 

        // Check if page is already in buffer pool
        // If found, increment pin count and return page, make it lru
        if (pageTable.containsKey(pageId)) {
            int frameIdx = pageTable.get(pageId);
            isPinned[frameIdx] += 1;

            updateLRUQueueWithMRUPage(pageId);

            if (printStuff) {
                System.out.println("Cache hit: Page " + pageId);
            }

            return bufferPool[frameIdx];
        }

        if (usedFrames >= bufferSize && !isPageEvicted()) {
            System.out.println("Error: Cannot load page " + pageId + ". Buffer is full, all pages are pinned.");
            throw new IllegalStateException("Buffer is full, all pages are pinned, cannot get page.");
        }

        page = readPageFromDisk(pageId);
    
        if (page != null) {
            if (printStuff) {
                System.out.println("Cache miss: Loaded Page " + pageId + " from disk");
            }

            int frameIdx = addPageToBufferPool(page);
            isPinned[frameIdx] = 1;
            updateLRUQueueWithMRUPage(pageId);
        } else {
            System.out.println("Error: Failed to load page " + pageId + " from disk");
        }

        return page;
    }

    @Override
    public void unpinPage(int pageId) {
        if (pageTable.containsKey(pageId)) {
            int frameIdx = pageTable.get(pageId);

            isPinned[frameIdx] = Math.max(0, isPinned[frameIdx] - 1);
        }
    }

    @Override
    public void markDirty(int pageId) {
        if (pageTable.containsKey(pageId)) {
            int frameIdx = pageTable.get(pageId);
            isDirty[frameIdx] = true;

            // if (printStuff) {
            //     System.out.println("Marked page " + pageId + " as dirty");
            // }
        }
    }

    // Updates LRU queue with most recently used page
    private void updateLRUQueueWithMRUPage(int pageId) {
        lruQueue.remove(pageId);
        lruQueue.put(pageId, false); // Add to LRU as most recently used
    }

    // Adds a page to buffer pool, increments usedFrames
    // @return frame index of added page
    private int addPageToBufferPool(Page page) {
        if (page == null || usedFrames >= bufferSize) {
            System.err.println("Error: Cannot add page to buffer pool. Page is null or buffer is full.");
            return -1;
        }

        int frameIdx = usedFrames++;
        bufferPool[frameIdx] = page;
        pageTable.put(page.getId(), frameIdx);

        if (printStuff) {
            System.out.println("Added page " + page.getId() + " to frame " + frameIdx);
        }
        
        return frameIdx;
    }

    // Writes page to disk if dirty
    // @return true if successful
    private boolean handleDirtyPage(int pageId, int frameIdx) {
        if (isDirty[frameIdx]) {
            if (!writePageToDisk(bufferPool[frameIdx])) {
                System.out.println("Error: Failed to write dirty page " + pageId + " to disk");

                return false;
            }

            if (printStuff) {
                System.err.println("Wrote page " + pageId + " to disk, marked undirty");
            }

            isDirty[frameIdx] = false;
        }

        return true;
    }

    private void fillEvictedPageFrame(int evictedPageFrame, int lastFrameIdx) {
        if (evictedPageFrame == lastFrameIdx || lastFrameIdx < 0) {
            return;
        }

        bufferPool[evictedPageFrame] = bufferPool[lastFrameIdx];
        isDirty[evictedPageFrame] = isDirty[lastFrameIdx];
        isPinned[evictedPageFrame] = isPinned[lastFrameIdx];
        pageTable.put(bufferPool[evictedPageFrame].getId(), evictedPageFrame); // update the frame index in the page table

        bufferPool[lastFrameIdx] = null;
        isDirty[lastFrameIdx] = false;
        isPinned[lastFrameIdx] = 0;
    }

    // Evicts the least recently used page from buffer pool
    // @return true if a page was evicted, false otherwise
    private boolean isPageEvicted() {
        if (lruQueue.isEmpty()) {
            return false; // No pages to evict
        }

        Iterator<Integer> iterator = lruQueue.keySet().iterator();

        while (iterator.hasNext()){
            int pageId = iterator.next();
            int frameIdx = pageTable.get(pageId);

            if (isPinned[frameIdx] == 0) {
                if (isDirty[frameIdx] && !handleDirtyPage(pageId, frameIdx)) {
                    System.out.println("Error: Failed to write dirty page " + pageId + " to disk");

                    return false;
                }

                // interchange the last frame with the frame to be evicted
                fillEvictedPageFrame(frameIdx, usedFrames - 1);

                pageTable.remove(pageId);
                lruQueue.remove(pageId);
                usedFrames--;

                if (printStuff) {
                    System.out.println("Evicted page " + pageId);
                }

                return true;
            }
        }

        return false;
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

            Page pageFromDisk = new BinaryPage(pageId);

            boolean success = pageFromDisk.deserialize(data); 
            if (!success) {
                System.out.println("Something went wrong in deserialization, there was not enough space.");
            } else {
                System.out.println("Deserialized page " + pageId + " from disk");
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
            // System.out.println("Wrote page " + page.getId() + " to disk");

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