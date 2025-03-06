import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Iterator;

public class LRUBufferManager extends BufferManager {

    private LinkedHashMap<Integer, Integer> frameMap;
    private Page[] bufferPool;
    private boolean[] isDirty;
    private int[] pinCount;
    private int pageCount;
    private String binaryFile = "data.bin";
    private int pageSize = 4096; // 4KB

    public LRUBufferManager(int numFrames) {
        super(numFrames);
        // LinkedHashMap parameters (initialCapacity, loadFactor, false means maintains insertion order. True means order by most recently accessed)
        //maybe change to true?
        frameMap = new LinkedHashMap<>(1 + (bufferSize * 4) / 3, 0.75f, false);
        bufferPool = new Page[bufferSize]; // all null
        isDirty = new boolean[bufferSize]; // all false
        pinCount = new int[bufferSize]; // all 0
        pageCount = 0;

        initFile(); // Initialize binary file if it doesn’t exist 
    }

    private void initFile() {
        File file = new File(binaryFile);
        if (!file.exists()) {
            try {
                if (file.createNewFile()) {
                    System.out.println("Created new file: " + binaryFile);
                } else {
                    System.out.println("Failed to create file: " + binaryFile);
                }
            } catch (IOException e) {
                System.err.println("Error creating file " + binaryFile + ": " + e.getMessage());
            }
        }
    }

    // Reads bytes from disk
    private Page readPageFromDisk(int pageId) {
        if (pageId > pageCount) {
            return null; // no such page in disk
        }

        try (RandomAccessFile raf = new RandomAccessFile(binaryFile, "r")) {
            raf.seek((long) pageId * pageSize);
            byte[] data = new byte[pageSize];
            raf.readFully(data);

            Page page = new UnnamedPage(pageId);
            // TODO: implement page.deserialize(data)

            return page;
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
    private boolean writePageToDisk(Page page) {
        try (RandomAccessFile raf = new RandomAccessFile(binaryFile, "rw")) {
            raf.seek((long) page.getId() * pageSize);
            raf.write(123); // TODO page.serialize() should return byte[]

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

    /**
     * Replaces page in bufferPool at frameIndex, writing to disk if necessary.
     * Ignores pins, and resets isDirty and pinCount at frameIndex.
     */
    private void overwriteFrame(int frameIndex, Page nextPage) throws IOException {
        Page prevPage = bufferPool[frameIndex];
        if (isDirty[frameIndex]) {
            writePageToDisk(prevPage);
        }
        frameMap.remove(prevPage.getId());
        bufferPool[frameIndex] = nextPage;
        isDirty[frameIndex] = false;
        pinCount[frameIndex] = 0;
        frameMap.put(frameIndex, nextPage.getId());
        prevPage = null;
    }

    /**
     * Doesn't change any instance variables.
     */
    private int leastRecentlyUsedFrame() {
        Iterator<Integer> lruPageIds = frameMap.keySet().iterator();
        while (lruPageIds.hasNext()) {
            int pageId = lruPageIds.next();
            int frameIndex = frameMap.get(pageId);
            if (pinCount[frameIndex] == 0) {
                return frameIndex;
            }
        }
        throw new IllegalStateException("Buffer contains no unpinned pages.");
    }

    @Override
    public Page getPage(int pageId) throws IOException {
        int frameIndex;
        if (frameMap.containsKey(pageId)) {
            frameIndex = frameMap.get(pageId);
            frameMap.remove(pageId); // remove so that insertion resets pageId's position in frameMap.keySet()
            frameMap.put(pageId, frameIndex);
        } else {
            frameIndex = leastRecentlyUsedFrame();
            Page nextPage = readPageFromDisk(pageId);
            overwriteFrame(frameIndex, nextPage);
        }
        pinCount[frameIndex] += 1;
        return bufferPool[frameIndex];
    }

    @Override
    public Page createPage() throws IOException {
        int pageId = pageCount++;
        Page pageObject = getPage(pageId); // inserts pageId into frameMap
        int frameIndex = frameMap.get(pageId);
        isDirty[frameIndex] = true;
        return pageObject;
    }

    @Override
    public void markDirty(int pageId) {
        if (frameMap.containsKey(pageId)) {
            int frameIndex = frameMap.get(pageId);
            isDirty[frameIndex] = true;
        }
        throw new IllegalArgumentException("No page with this ID is in the buffer.");
    }

    @Override
    public void unpinPage(int pageId) {
        if (frameMap.containsKey(pageId)) {
            int frameIndex = frameMap.get(pageId);
            if (pinCount[frameIndex] == 0) {
                throw new IllegalStateException("Cannot unpin page with no pins.");
            }
            pinCount[frameIndex] -= 1;
        }
        throw new IllegalArgumentException("No page with this ID is in the buffer.");
    }

}
