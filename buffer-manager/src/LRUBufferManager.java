import java.util.Map;
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
        bufferPool = new Page[bufferSize];
        isDirty = new boolean[bufferSize];
        pinCount = new int[bufferSize];
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
    private boolean writePageToDisk(UnnamedPage page) throws FileNotFoundException, IOException {
        try (RandomAccessFile raf = new RandomAccessFile(binaryFile, "rw")) {
            raf.seek((long) pageSize); // IMO, page should have pageId, pageRows (all the rows in the page)
            raf.write(123); // TODO: implement serializer in Page; serialize(data)
        }

        return true;
    }

    private int getEmptyFrame() {
        return 0;
    }

    @Override
    public Page getPage(int pageId) {
        int frameIndex = -1;
        if (frameMap.containsKey(pageId)) {
            frameIndex = frameMap.get(pageId);
            frameMap.remove(pageId); // removal to allow reinsertion
        } else {
            Iterator<Integer> lruPageIds = frameMap.keySet().iterator();
            while (true) { // loop to find page eligible for eviction
                if (!lruPageIds.hasNext()) {
                    // throw exception
                }
                int pId = lruPageIds.next();
                int fIdx = frameMap.get(pId);
                if (pinCount[fIdx] == 0) { // if page can be removed
                    if (isDirty[fIdx]) {
                        writePageToDisk(pId, bufferPool[fIdx]);
                    }
                    frameIndex = fIdx;
                    break;
                }
            }
            bufferPool[frameIndex] = readPageFromDisk(pageId);
        }
        frameMap.put(pageId, frameIndex);
        pinCount[frameIndex] += 1;
        return bufferPool[frameIndex];
    }

    @Override
    public Page createPage() {
        int pageId = pageCount++;
        Page pageObject = getPage(pageId);
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
        // else throw error
    }

    @Override
    public void unpinPage(int pageId) {
        if (frameMap.containsKey(pageId)) {
            int frameIndex = frameMap.get(pageId);
            pinCount[frameIndex] -= 1; // check if pin count is zero before decrement? throw error?
        }
        // else throw error?
    }

}
