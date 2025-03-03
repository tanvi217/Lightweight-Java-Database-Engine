import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;

public class BufferManagerLRU extends BufferManager {
    private Map<Integer, Integer> frameMap;
    private boolean[] isDirty;
    private int[] pinCount;
    private int currentPage;

    public BufferManagerLRU(int numFrames) {
        super(numFrames);
        frameMap = new LinkedHashMap<>(1 + (bufferSize * 4) / 3, 0.75f, false);
        isDirty = new boolean[bufferSize];
        pinCount = new int[bufferSize];
        currentPage = 0;
    }

    @Override
    Page getPage(int pageId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getPage'");
    }

    @Override
    Page createPage() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createPage'");
    }

    @Override
    void markDirty(int pageId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'markDirty'");
    }

    @Override
    void unpinPage(int pageId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'unpinPage'");
    }
}