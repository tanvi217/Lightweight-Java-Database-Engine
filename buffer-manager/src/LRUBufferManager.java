import java.util.Map;
import java.util.LinkedHashMap;

public class LRUBufferManager extends BufferManager {

    private Map<Integer, Integer> frameMap;
    private Page[] bufferPool;
    private boolean[] isDirty;
    private int[] pinCount;
    private int pageCount;

    public LRUBufferManager(int numFrames) {
        super(numFrames);
        frameMap = new LinkedHashMap<>(1 + (bufferSize * 4) / 3, 0.75f, false);
        bufferPool = new Page[bufferSize];
        isDirty = new boolean[bufferSize];
        pinCount = new int[bufferSize];
        pageCount = 0;
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
