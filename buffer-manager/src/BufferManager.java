import java.util.Random;

public abstract class BufferManager {

    protected final int bufferSize;
    private String defaultFileTitle;
    private static Random rand = new Random(1L);

    /**
     * @param bufferSize Configurable size of buffer cache.
     */
    public BufferManager(int bufferSize) {
        this.bufferSize = bufferSize;
        defaultFileTitle = "bm-" + rand.nextInt((int) 1e9);
    }

    /**
     * Fetches a page from memory if available; otherwise, loads it from disk.
     * The page is immediately pinned.
     *
     * Piazza @40 - "You are right that in this specific use case with no
     * updates or deletes a caller that invokes getPage will never mark it
     * dirty."
     *
     * Piazza @50 - "The file is only updated when dirty pages are evicted and
     * written back by the buffer manager. You can only evict a page if its pin
     * count is 0. The pin count is incremented for every invocation of getPage.
     * You should evict the LRU page among the set of unpinned ones."
     *
     * Piazza @62 - "A page is considered "used" when createPage or getPage is
     * invoked on that page. Other events do not update the LRU status."
     *
     * @param pageId    The ID of the page to fetch from the given file.
     * @param fileTitle A string representing the file, but not a full file path.
     * @return The Page object whose content is stored in a frame of the buffer
     *         pool manager.
     */
    abstract Page getPage(int pageId, String fileTitle);

    /**
     * Creates a new page. The page is immediately pinned.
     *
     * Piazza @54: "A page should be marked as dirty by the buffer manager
     * inside the createPage method."
     *
     * Piazza @65: The buffer should maintain an increasing counter of the
     * previous pages to create new page IDs.
     *
     * @return The Page object whose content is stored in a frame of the buffer
     *         pool manager.
     */
    abstract Page createPage(String fileTitle, int rowLength);

    /**
     * Marks a page as dirty, indicating it needs to be written to disk before
     * eviction.
     *
     * Piazza @54: Q - What happens if the caller uses markDirty on a page which
     * is not in the buffer? A - "It would return an error."
     */
    abstract void markDirty(int pageId, String fileTitle);

    abstract void markClean(int pageId, String fileTitle);

    /**
     * Unpins a page in the buffer pool, allowing it to be evicted if necessary.
     *
     * Piazza @50: "The buffer manager increments the pin count when a page is
     * requested through the getPage or createPage method. It decrements the pin
     * count when the caller invokes unpinPage, which indicates that the caller
     * does not need the page any longer."
     *
     * Piazza @54: Q - "When can we unpin the page?" A - "The caller invokes
     * unpinPage when it is done using it. The buffer manager must be robust to
     * the caller calling unpinPage at any time."
     */
    abstract void unpinPage(int pageId, String fileTitle);

    /**
     * Ensure that all dirty pages currently in memory are written back to disk.
     * 
     * (Check if this is right) We set the pin count of each page to zero, and
     * mark all pages as not dirty.
     */
    abstract void force();

    /*
     * These methods preserve the BufferManager API from Lab 1, passing a randomly
     * generated default file title. The row length used in createPage is the
     * specific row length we used for the IMDb data.
     */
    public Page getPage(int pageId) { return getPage(pageId, defaultFileTitle); }
    public Page createPage() { return createPage(defaultFileTitle, Constants.IMDB_ROW_LENGTH); }
    public void markDirty(int pageId) { markDirty(pageId, defaultFileTitle); }
    public void markClean(int pageId) { markClean(pageId, defaultFileTitle); }
    public void unpinPage(int pageId) { unpinPage(pageId, defaultFileTitle); }

}
