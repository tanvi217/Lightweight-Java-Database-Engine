import java.util.Map;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;

public class LRUBufferManager extends BufferManager {

    private Map<Integer, Integer> frameMap;
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
    Page getPage(int pageId) {
        int frameIndex = frameMap.containsKey(pageId) ? frameMap.get(pageId) : getEmptyFrame();
        pinCount[frameIndex] += 1;
        frameMap.remove(pageId);
        frameMap.put(pageId, frameIndex);
        return null; // return page object
    }

    @Override
    Page createPage() {
        int pageId = pageCount++;
        return getPage(pageId);
    }

    @Override
    void markDirty(int pageId) {
        if (frameMap.containsKey(pageId)) {
            int frameIndex = frameMap.get(pageId);
            isDirty[frameIndex] = true;
        }
    }

    @Override
    void unpinPage(int pageId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
