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
     * @param pageId    The ID of the page to fetch from the given file.
     * @param fileTitle A string representing the file, but not a full file path.
     * @return The Page object whose content is stored in a frame of the buffer
     *         pool manager.
     */
    abstract Page getPage(int pageId, String fileTitle);

    /**
     * Creates a new page. The page is immediately pinned.
     * @return The Page object whose content is stored in a frame of the buffer
     *         pool manager.
     */
    abstract Page createPage(String fileTitle, int rowLength);

    /**
     * Marks a page as dirty, indicating it needs to be written to disk before
     * eviction.
     */
    abstract void markDirty(int pageId, String fileTitle);

    abstract void markClean(int pageId, String fileTitle);

    /**
     * Unpins a page in the buffer pool, allowing it to be evicted if necessary.
     */
    abstract void unpinPage(int pageId, String fileTitle);

    /**
     * Ensure that all dirty pages currently in memory are written back to disk.
     * 
     * (Check if this is right) We set the pin count of each page to zero, and
     * mark all pages as not dirty.
     */
    abstract void force();

    abstract int getPageCount(String fileTitle);

    abstract void setPageCount(int pageCount, String fileTitle);

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
