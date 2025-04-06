// if the file title "BTree" is not accessed by any other caller, then we could add some BTree id 0,1,2... to the file name to make sure different instance of BTree use different files
// should the key length be set in the constructor somehow? (so not fixed)

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class Main {

    private BufferManager bm;
    private int rootPid;

    private int treeDepth;
    private int keyLength;
    private String fileTitle;

    private byte[] dataAfterKey(Page nodePage, int rowId) {
        byte[] rowData = nodePage.getRow(rowId).data;
        return Arrays.copyOfRange(rowData, keyLength, rowData.length);
    }

    private int compareKeyToRow(byte[] key, Page nodePage, int rowId) {
        byte[] rowData = nodePage.getRow(rowId).data;
        return Arrays.compare(key, 0, keyLength, rowData, 0, keyLength);
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

    public byte[] getKeyFromString(String keyString) {
        return Arrays.copyOf(keyString.getBytes(StandardCharsets.UTF_8), keyLength);
    }

    private List<Rid> internalRangeSearch(byte[] startKey, byte[] endKey) {
        List<Rid> matches = new ArrayList<>();

        return matches;
    }

    public Iterator<Rid> search(String keyString) {
        byte[] key = getKeyFromString(keyString);
        return internalRangeSearch(key, key).iterator();
    }

    public Iterator<Rid> rangeSearch(String startKeyString, String endKeyString) {
        byte[] startKey = getKeyFromString(startKeyString);
        byte[] endKey = getKeyFromString(endKeyString);
        return internalRangeSearch(startKey, endKey).iterator();
    }

}
