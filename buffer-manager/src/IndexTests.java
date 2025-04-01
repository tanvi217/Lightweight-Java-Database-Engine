import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class IndexTests {

    public static BTreeIndex<String> CreateIndex(BufferManager bm, int attributeIndex) {
        BTreeIndex<String> btree = new BTreeIndex<String>(4);
        int pageId = 0;

        while (true) {
            try {
                Page page = bm.getPage(pageId);

                for (int rowId = 0; rowId < Constants.MAX_PAGE_ROWS; rowId++) {
                    try {
                        Row row = page.getRow(rowId);
                        String key = new String(row.getAttribute(attributeIndex), StandardCharsets.UTF_8).trim();
                        btree.insert(key, new Rid(pageId, rowId));
                        rowId++;
                    }
                    catch (Exception ex) { }
                }
    
                bm.unpinPage(pageId);
                pageId++;
            }
            catch (Exception e) {
                break;
            }
        }

        bm.force();

        System.out.println("Built index");
        return btree;
    }

    public static void verifyIndex(BTreeIndex<String> bTreeIndex, BufferManager bm, String key, int attributeIndex) {
        Iterator<Rid> rids = bTreeIndex.search(key);

        if (!rids.hasNext()) {
            System.out.println("Couldn't find key: " + key);
        }
        
        while (rids.hasNext()) {
            Rid rid = rids.next();
            Page page = bm.getPage(rid.getPageId());
            Row row = page.getRow(rid.getSlotId());
            String foundKeyValue = new String(row.getAttribute(attributeIndex)).trim();

            if (!foundKeyValue.equals(key)) {
                System.err.println("Couldn't find key: Search key: " + key + ", Found key: " + foundKeyValue +
                    " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            } else {
                System.out.println("Found: " + foundKeyValue + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            }

            bm.unpinPage(rid.getPageId());
        }
    }

    /* TODO: Implement rangeSearch in BPlusTree */
    // private static void verifyRange(BTreeIndex<String> bTreeIndex, BufferManager bm, String key, int attributeIndex, String startKey, String endKey) {
    //     Iterator<Rid> rids = bTreeIndex.rangeSearch(startKey, endKey);

    //     if (!rids.hasNext()) {
    //         System.out.println("Couldn't find entries in the range: " + startKey + ", " + endKey);
    //     }

    //     while (rids.hasNext()) {
    //         Rid rid = rids.next();
    //         Page page = bm.getPage(rid.getPageId());
    //         Row row = page.getRow(rid.getSlotId());
    //         String foundKeyValue = new String(row.getAttribute(attributeIndex)).trim();

    //         if (!foundKeyValue.equals(key)) {
    //             System.err.println("Found key out of given range: (startKey, endKey): (" + startKey + ", " + endKey 
    //                 + "), Found key: " + foundKeyValue + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
    //         } else {
    //             System.out.println("Found: " + foundKeyValue + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
    //         }

    //         bm.unpinPage(rid.getPageId());
    //     }
    // }
}