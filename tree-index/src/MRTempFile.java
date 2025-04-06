// if the file title "BTree" is not accessed by any other caller, then we could add some BTree id 0,1,2... to the file name to make sure different instance of BTree use different files
// should the key length be set in the constructor somehow? (so not fixed)

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class MRTempFile<K> {

    private static int numInstances = 0;
    private BufferManager bm;
    private int rootPid;

    private int treeDepth;
    private int bytesInKey;
    private String fileTitle;

    public MRTempFile(BufferManager bm, int bytesInKey) {
        this.bm = bm;
        this.bytesInKey = bytesInKey;
        treeDepth = 1;
        fileTitle = "B-plus-tree-" + (++numInstances);
        
        rootPid = 0; // call a method to create leaf node? which returns Pid of created page
    }

    private byte[] dataAfterKey(Page nodePage, int rowId) {
        byte[] rowData = nodePage.getRow(rowId).data;
        return Arrays.copyOfRange(rowData, bytesInKey, rowData.length);
    }

    private int compareKeyToRow(byte[] key, Page nodePage, int rowId) {
        byte[] rowData = nodePage.getRow(rowId).data;
        return Arrays.compare(key, 0, bytesInKey, rowData, 0, bytesInKey);
    }

    private int findInNonLeafPage(byte[] key, int pageId) {
        Page nodePage = bm.getPage(pageId, fileTitle);
        byte[] leftPointer = dataAfterKey(nodePage, 0); // pageId bytes of first row
        int rowId = 1; // start loop at second row
        while (rowId < nodePage.height()) {
            if (compareKeyToRow(key, nodePage, rowId) <= 0) { // compare key to current row's key
                break;
            }
            leftPointer = dataAfterKey(nodePage, rowId); // pageId bytes of current row
            ++rowId;
        } // if break is never reached, the loop ends with leftPointer being the pageId of the final row
        bm.unpinPage(pageId, fileTitle);
        return ByteBuffer.wrap(leftPointer).getInt(); // convert pageId bytes to int
    }

    public byte[] getKeyFromComparable(K callerKey) {
        if (callerKey instanceof String) {
            String keyString = (String) callerKey;
            return Arrays.copyOf(keyString.getBytes(StandardCharsets.UTF_8), bytesInKey);
        } else if (callerKey instanceof Integer) {
            int keyInt = (Integer) callerKey;
            return ByteBuffer.allocate(4).putInt(keyInt).array();
        } else {
            throw new IllegalArgumentException();
        }
    }

    private int getIntFromRow(Page nodePage, int rowId, int index) {
        byte[] rowData = nodePage.getRow(rowId).data;
        return ByteBuffer.wrap(rowData).getInt(index * 4);
    }

    private List<Rid> getLeafMatches(int leafPid, byte[] startKey, byte[] endKey) {
        List<Rid> matches = new ArrayList<>();
        Page leafPage = bm.getPage(leafPid, fileTitle);
        int pageHeight = leafPage.height();
        int rowId = 1; // start at second row
        while (rowId < pageHeight && compareKeyToRow(startKey, leafPage, rowId) < 0) {
            ++rowId;
        }
        while (rowId < pageHeight && compareKeyToRow(endKey, leafPage, rowId) >= 0) {
            matches.add(new Rid(dataAfterKey(leafPage, rowId)));
            ++rowId;
        }
        boolean unfinished = rowId == pageHeight;
        int nextLeafPid = unfinished ? getIntFromRow(leafPage, rowId, 1) : 0;
        bm.unpinPage(leafPid, fileTitle);
        if (unfinished) { // if last row still matches, some matches may be in next leaf
            matches.addAll(getLeafMatches(nextLeafPid, startKey, endKey));
        }
        return matches;
    }

    private int[] getSearchPath(byte[] key) {
        int[] searchPath = new int[treeDepth];
        int currentPid = rootPid;
        int depthIndex = 0;
        while (depthIndex < treeDepth - 1) {
            searchPath[depthIndex] = currentPid;
            currentPid = findInNonLeafPage(key, currentPid);
            ++depthIndex;
        }
        searchPath[depthIndex] = currentPid;
        return searchPath;
    }

    private Iterator<Rid> internalRangeSearch(byte[] startKey, byte[] endKey) {
        int pageId = getSearchPath(startKey)[treeDepth - 1]; // leaf node is last in search path
        return getLeafMatches(pageId, startKey, endKey).iterator();
    }

    public Iterator<Rid> search(K callerKey) {
        byte[] key = getKeyFromComparable(callerKey);
        return internalRangeSearch(key, key);
    }

    public Iterator<Rid> rangeSearch(K callerStartKey, K callerEndKey) {
        byte[] startKey = getKeyFromComparable(callerStartKey);
        byte[] endKey = getKeyFromComparable(callerEndKey);
        return internalRangeSearch(startKey, endKey);
    }

    public void insert(K callerKey, Rid rid) {
        byte[] key = getKeyFromComparable(callerKey);
        int[] searchPath = getSearchPath(key);
    }

}
