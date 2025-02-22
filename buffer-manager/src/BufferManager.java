
public abstract class BufferManager {

    // configurable size of buffer cache.
    final int bufferSize;

    public BufferManager(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    /**
     * Fetches a page from memory if available; otherwise, loads it from disk.
     * The page is immediately pinned.
     *
     * @param pageId The ID of the page to fetch.
     * @return The Page object whose content is stored in a frame of the buffer
     * pool manager.
     */
    abstract Page getPage(int pageId);

    /**
     * Creates a new page. The page is immediately pinned.
     *
     * @return The Page object whose content is stored in a frame of the buffer
     * pool manager.
     */
    abstract Page createPage();

    /**
     * Marks a page as dirty, indicating it needs to be written to disk before
     * eviction.
     *
     * @param pageId The ID of the page to mark as dirty.
     */
    abstract void markDirty(int pageId);

    /**
     * Unpins a page in the buffer pool, allowing it to be evicted if necessary.
     *
     * @param pageId The ID of the page to unpin.
     */
    abstract void unpinPage(int pageId);
}
