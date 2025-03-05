import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;
import java.util.Iterator;

public class LRUBufferManager extends BufferManager {

    private LinkedHashMap<Integer, Integer> frameMap;
    private Page[] bufferPool;
    private boolean[] isDirty;
    private int[] pinCount;
    private int pageCount;
    private String binaryFile = "binaryFile.bin";
    private int pageSize = 4096; // 4KB

    public LRUBufferManager(int numFrames) {
        super(numFrames);
        frameMap = new LinkedHashMap<>(1 + (bufferSize * 4) / 3, 0.75f, false);
        bufferPool = new Page[bufferSize]; // all null
        isDirty = new boolean[bufferSize]; // all false
        pinCount = new int[bufferSize]; // all 0
        pageCount = 0;
    }

    // Reads bytes from disk (binaryFile), deserializes as Page
    private Page readPageFromDisk(int pageId) throws FileNotFoundException, IOException {
        try (RandomAccessFile raf = new RandomAccessFile(binaryFile, "r")) {
            raf.seek((long) pageId * pageSize);
            byte[] data = new byte[pageSize];
            raf.readFully(data);

            return new UnnamedPage(); // TODO: implement deserializer in Page; deserialize(pageId, data)
        }
    }

    // Returns true is flushed to disk; caller to mark the page dirty and update lru
    private boolean writePageToDisk(Page page) throws FileNotFoundException, IOException {
        try (RandomAccessFile raf = new RandomAccessFile(binaryFile, "rw")) {
            raf.seek((long) pageSize); // IMO, page should have pageId, pageRows (all the rows in the page)
            raf.write(123); // TODO: implement serializer in Page; serialize(data)
        }

        return true;
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
        frameMap.remove(-1); // replace -1 with something like prevPage.getId()
        bufferPool[frameIndex] = nextPage;
        isDirty[frameIndex] = false;
        pinCount[frameIndex] = 0;
        frameMap.put(frameIndex, -1); // replace -1 with something like nextPage.getId()
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
