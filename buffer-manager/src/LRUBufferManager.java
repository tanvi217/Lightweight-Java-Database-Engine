import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;

public class LRUBufferManager extends BufferManager {

    private Map<Integer, Integer> frameMap;
    private List<Page> allPages;
    private boolean[] isDirty;
    private int[] pinCount;

    public LRUBufferManager(int numFrames) {
        super(numFrames);
        frameMap = new LinkedHashMap<>(1 + (bufferSize * 4) / 3, 0.75f, false);
        allPages = new ArrayList<>();
        isDirty = new boolean[bufferSize];
        pinCount = new int[bufferSize];
    }

    private int getEmptyFrame() {
        return 0;
    }

    @Override
    Page getPage(int pageId) {
        Page target = allPages.get(pageId);
        int frameIndex = frameMap.containsKey(pageId) ? frameMap.get(pageId) : getEmptyFrame();
        pinCount[frameIndex] += 1;
        frameMap.remove(pageId);
        frameMap.put(pageId, frameIndex);
        return target;
    }

    @Override
    Page createPage() {
        int pageId = allPages.size();
        allPages.add(new UnnamedPage());
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
