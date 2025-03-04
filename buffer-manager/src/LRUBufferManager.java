import java.util.LinkedHashMap;
import java.util.Iterator;

public class LRUBufferManager extends BufferManager {

    private LinkedHashMap<Integer, Integer> frameMap;
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
