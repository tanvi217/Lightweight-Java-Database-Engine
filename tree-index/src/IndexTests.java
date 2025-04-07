import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IndexTests {

    public static MRTempFile<String> CreateIndex(BufferManager bm, int attributeIndex, int pinnedPageCount) {
        MRTempFile<String> btree = new MRTempFile<String>(bm, 9, pinnedPageCount);
        int pageId = 0;

        while (true) {
            try {
                Page page = bm.getPage(pageId);
                
                for (int rowId = 0; rowId < page.height(); rowId++) {
                    try {
                        Row row = page.getRow(rowId);
                        String key = new String(row.getAttribute(attributeIndex), StandardCharsets.UTF_8).trim();
                        btree.insert(key, new Rid(pageId, rowId));
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

        System.out.println("Built index for attributeIndex: " + attributeIndex);
        return btree;
    }

    public static void verifyIndex(MRTempFile<String> bTreeIndex, BufferManager bm, String key, int attributeIndex) {
        System.out.println("\nVerifying index for key: " + key + " at attribute index: " + attributeIndex);
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

    public static void verifyRange(MRTempFile<String> bTreeIndex, BufferManager bm, String startKey, String endKey, int attributeIndex) {
        System.out.println("\nVerifying index for range: " + startKey + ", " + endKey + " at attribute index: " + attributeIndex);

        Iterator<Rid> rids = bTreeIndex.rangeSearch(startKey, endKey);

        if (!rids.hasNext()) {
            System.out.println("Couldn't find entries in the range: " + startKey + ", " + endKey);
        }

        while (rids.hasNext()) {
            Rid rid = rids.next();
            Page page = bm.getPage(rid.getPageId());
            Row row = page.getRow(rid.getSlotId());
            String foundKeyValue = new String(row.getAttribute(attributeIndex)).trim();

            if (foundKeyValue.compareTo(startKey) < 0 || foundKeyValue.compareTo(endKey) > 0) {
                System.err.println("Found key out of given range: (startKey, endKey): (" + startKey + ", " + endKey 
                    + "), Found key: " + foundKeyValue + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            } else {
                System.out.println("Found: " + foundKeyValue + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            }

            bm.unpinPage(rid.getPageId());
        }
    }

    public static void compareRangeSearch(MRTempFile<String> bTreeIndex, BufferManager bm, String[][] ranges, int attributeIndex, int totalRowsInTable) {
        List<Double> selectivities = new ArrayList<>();
        List<Double> scanTimes = new ArrayList<>();
        List<Double> indexTimes = new ArrayList<>();

        for (String[] range : ranges) {
            System.out.println("\nComparing sequential and index range queries for keys: " + range[0] + ", " + range[1]);
            String startKey = range[0];
            String endKey = range[1];

            // Sequential scan
            long startTime = System.nanoTime();
            List<Row> scanResults = scanTable(bm, attributeIndex, startKey, endKey);
            long scanTime = System.nanoTime() - startTime;
            scanTimes.add(scanTime / 1e6); // msec

            // Index range query
            startTime = System.nanoTime();
            Iterator<Rid> rids = bTreeIndex.rangeSearch(startKey, endKey);
            List<Row> indexResults = fetchRows(bm, rids);
            long indexTime = System.nanoTime() - startTime;
            indexTimes.add(indexTime / 1e6); // msec

            // selectivity
            double selectivity = (double) scanResults.size() / totalRowsInTable;
            selectivities.add(selectivity);

            System.out.println("Range [" + startKey + ", " + endKey + "]: " +
                "Scan rows = " + scanResults.size() + ", Index rows = " + indexResults.size());
        }

        // Output results
        plotResults(selectivities, scanTimes, indexTimes);
    }

    private static List<Row> scanTable(BufferManager bm, int attributeIndex, String startKey, String endKey) {
        List<Row> results = new ArrayList<>();
        int pageId = 0;

        while (true) {
            try {
                Page page = bm.getPage(pageId);

                for (int rowId = 0; rowId < Constants.MAX_PAGE_ROWS; rowId++) {
                    try {
                        Row row = page.getRow(rowId);
                        String key = new String(row.getAttribute(attributeIndex), StandardCharsets.UTF_8).trim();

                        if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {
                            results.add(row);
                        }
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

        return results;
    }

    private static List<Row> fetchRows(BufferManager bm, Iterator<Rid> rids) {
        List<Row> results = new ArrayList<>();

        while (rids.hasNext()) {
            Rid rid = rids.next();
            Page page = bm.getPage(rid.getPageId());
            Row row = page.getRow(rid.getSlotId());
            results.add(row);
            bm.unpinPage(rid.getPageId());
        }

        return results;
    }

    private static void plotResults(List<Double> selectivities, List<Double> scanTimes, List<Double> indexTimes) {
        System.out.println("\nSelectivity   | Scan Time (ms)   | Index Time (ms)   | Ratio (Scan/Index)");
        System.out.println("-------------------------------------------------");

        for (int i = 0; i < selectivities.size(); i++) {
            double ratio = scanTimes.get(i) / indexTimes.get(i);
            System.out.printf("%.4f       | %.4f         | %.4f          | %.4f%n",
                selectivities.get(i), scanTimes.get(i), indexTimes.get(i), ratio);
        }
    }
}