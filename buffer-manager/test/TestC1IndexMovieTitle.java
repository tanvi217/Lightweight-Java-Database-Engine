import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class TestC1IndexMovieTitle {
    public static void main(String[] args) {
        LRUBufferManager bm = new LRUBufferManager(10);
        BTreeIndex<String> btree = new BTreeIndex<String>(3);
        int pageId = 0;

        while (true) {
            try {
                Page page = bm.getPage(pageId, Constants.BINARY_DATA_FILE);

                for (int rowId = 0; rowId < Constants.MAX_PAGE_ROWS; rowId++) {
                    try {
                        Row row = page.getRow(rowId);
                        String title = new String(row.getAttribute(Constants.TITLE_INDEX), StandardCharsets.UTF_8).trim();
                        btree.insert(title, new Rid(pageId, rowId));
                        rowId++;
                    }
                    catch (Exception ex) {
                        System.err.println("Exception while getting row");
                    }                
                }
    
                bm.unpinPage(pageId, Constants.BINARY_DATA_FILE);
                pageId++;
            }
            catch (Exception e) {
                break;
            }
        }

        bm.force();

        System.out.println("Built title index");
        verifyIndex(btree, bm);
    }

    private static void verifyIndex(BTreeIndex<String> titleIndex, LRUBufferManager bm) {
        Iterator<Rid> rids = titleIndex.search("Boxing");
        
        while (rids.hasNext()) {
            Rid rid = rids.next();
            Page page = bm.getPage(rid.getPageId(), Constants.INDEX_DATA_FILE);
            Row row = page.getRow(rid.getSlotId());
            String title = new String(row.getAttribute(Constants.TITLE_INDEX)).trim();
            System.out.println("Found: " + title + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            bm.unpinPage(rid.getPageId(), "test.bin");
        }
    }
}