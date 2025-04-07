import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IndexTests {

    public static BufferBTree<String> CreateIndex(BufferManager bm, int bytesInKey, int attS, int attL, int pinnedPageDepth, boolean debugPrinting) {
        BufferBTree<String> btree = new BufferBTree<String>(bm, bytesInKey, pinnedPageDepth, debugPrinting);
        int pageId = 0;

        while (true) {
            try {
                Page page = bm.getPage(pageId);
                
                for (int rowId = 0; rowId < page.height(); rowId++) {
                    try {
                        Row row = page.getRow(rowId);
                        String key = new String(row.getAttribute(attS, attL), StandardCharsets.UTF_8).trim();
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

        System.out.println("Built index for attribute starting at byte: " + attS);
        return btree;
    }

    public static void verifyIndex(BufferBTree<String> bTreeIndex, BufferManager bm, String key, int attS, int attL) {
        System.out.println("\nVerifying index for key: " + key + " at attribute  starting at byte: " + attS);
        Iterator<Rid> rids = bTreeIndex.search(key);

        if (!rids.hasNext()) {
            System.out.println("Couldn't find key: " + key);
        }
        
        while (rids.hasNext()) {
            Rid rid = rids.next();
            Page page = bm.getPage(rid.getPageId());
            Row row = page.getRow(rid.getSlotId());
            String foundKeyValue = new String(row.getAttribute(attS, attL)).trim();

            if (!foundKeyValue.isEmpty() && !foundKeyValue.equals(key)) {
                System.err.println("Couldn't find key: Search key: " + key + ", Found key: " + foundKeyValue +
                    " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            } else if (!foundKeyValue.isEmpty()) {
                // System.out.println("Found: " + foundKeyValue + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            }

            bm.unpinPage(rid.getPageId());
        }
    }

    public static void verifyRange(BufferBTree<String> bTreeIndex, BufferManager bm, String startKey, String endKey, int attS, int attL) {
        System.out.println("\nVerifying index for range: " + startKey + ", " + endKey + " at attribute  starting at byte: " + attS);

        Iterator<Rid> rids = bTreeIndex.rangeSearch(startKey.trim(), endKey.trim());

        if (!rids.hasNext()) {
            System.out.println("Couldn't find entries in the range: " + startKey + ", " + endKey);
        }

        while (rids.hasNext()) {
            Rid rid = rids.next();
            Page page = bm.getPage(rid.getPageId());
            Row row = page.getRow(rid.getSlotId());
            String foundKeyValue = new String(row.getAttribute(attS, attL)).trim();

            if (!foundKeyValue.isEmpty() && (foundKeyValue.compareToIgnoreCase(startKey) < 0 || foundKeyValue.compareToIgnoreCase(endKey) > 0)) {
                System.err.println("Found key out of given range: (startKey, endKey): (" + startKey + ", " + endKey 
                    + "), Found key: " + foundKeyValue + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            }
            if (!foundKeyValue.isEmpty()) {
                // System.out.println("Found: " + foundKeyValue + " at page " + rid.getPageId() + ", slot " + rid.getSlotId());
            }

            bm.unpinPage(rid.getPageId());
        }
    }

    public static void compareRangeSearch(BufferBTree<String> bTreeIndex, BufferManager bm, String[][] ranges, int attS, int attL, int totalRowsInTable) {
        List<Double> selectivities = new ArrayList<>();
        List<Double> scanTimes = new ArrayList<>();
        List<Double> indexTimes = new ArrayList<>();

        for (String[] range : ranges) {
            System.out.println("\nComparing sequential and index range queries for keys: " + range[0] + ", " + range[1]);
            String startKey = range[0];
            String endKey = range[1];

            // Sequential scan
            long startTime = System.nanoTime();
            List<Row> scanResults = scanTable(bm, attS, attL, startKey.trim(), endKey.trim());
            long scanTime = System.nanoTime() - startTime;
            scanTimes.add(scanTime / 1e6); // msec

            System.out.println("Sequential scan took " + scanTime / 1e6 + " ms, found " + scanResults.size() + " rows.");

            // Index range query
            startTime = System.nanoTime();
            Iterator<Rid> rids = bTreeIndex.rangeSearch(startKey.trim(), endKey.trim());
            List<Row> indexResults = fetchRows(bm, rids, attS, attL);
            long indexTime = System.nanoTime() - startTime;
            indexTimes.add(indexTime / 1e6); // msec

            // selectivity
            double selectivity = (double) scanResults.size() / totalRowsInTable;
            selectivities.add(selectivity);

            System.out.println("Index scan took " + indexTime / 1e6 + " ms, found " + indexResults.size() + " rows.");
        }

        // Output results
        plotResults(selectivities, scanTimes, indexTimes);
    }

    private static List<Row> scanTable(BufferManager bm, int attS, int attL, String startKey, String endKey) {
        List<Row> results = new ArrayList<>();
        int pageId = 0;

        while (true) {
            try {
                Page page = bm.getPage(pageId);

                for (int rowId = 0; rowId < page.height(); rowId++) {
                    try {
                        Row row = page.getRow(rowId);
                        String key = new String(row.getAttribute(attS, attL), StandardCharsets.UTF_8).trim();

                        if (attS == Constants.MOVIE_ID_START_BYTE) {
                            key = key.substring(0, Constants.MOVIE_ID_SIZE).trim();
                        }

                        if (key.compareToIgnoreCase(startKey) >= 0 && key.compareToIgnoreCase(endKey) <= 0) {
                            // System.out.println("Found: " + key + " at page " + pageId + ", slot " + rowId);
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

    private static List<Row> fetchRows(BufferManager bm, Iterator<Rid> rids, int attS, int attL) {
        List<Row> results = new ArrayList<>();

        while (rids.hasNext()) {
            Rid rid = rids.next();

            try {
                Page page = bm.getPage(rid.getPageId());
                Row row = page.getRow(rid.getSlotId());

                String foundKeyValue = new String(row.getAttribute(attS, attL)).trim();

                if (attS == Constants.MOVIE_ID_START_BYTE) {
                    foundKeyValue = foundKeyValue.substring(0, Constants.MOVIE_ID_SIZE).trim();
                }

                if (!foundKeyValue.isEmpty()) {
                    results.add(row);
                }
            } catch (Exception e) {
                continue;
            }

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