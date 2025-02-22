
public class UnnamedBufferManager extends BufferManager {

    public UnnamedBufferManager(int bufferSize) {
        super(bufferSize);
    }

    @Override
    Page getPage(int pageId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    Page createPage() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    void markDirty(int pageId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    void unpinPage(int pageId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
