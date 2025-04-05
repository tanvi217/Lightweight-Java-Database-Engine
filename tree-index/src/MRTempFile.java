// if the file title "BTree" is not accessed by any other caller, then we could add some BTree id 0,1,2... to the file name to make sure different instance of BTree use different files
// should the key length be set in the constructor somehow? (so not fixed)

import java.nio.ByteBuffer;
import java.util.Arrays;

class Main {

    private BufferManager bm;

    private int treeDepth;
    private int keyLength;
    private String fileTitle;
    // private static byte[] maxKey;

    private byte[][] splitRow(Page nodePage, int rowId) {
        byte[] rowData = nodePage.getRow(rowId).data;
        byte[] key = Arrays.copyOf(rowData, keyLength);
        byte[] value = Arrays.copyOfRange(rowData, keyLength, rowData.length);
        return new byte[][]{ key, value };
    }

    private int findInNonLeafPage(byte[] key, int pageId) {
        TabularPage nodePage = (TabularPage) bm.getPage(pageId, fileTitle);
        byte[] leftPage = splitRow(nodePage, 0)[1];
        int rowId = 1; // start at second row
        while (rowId < nodePage.get_nextRowId()) {
            byte[][] keyValue = splitRow(nodePage, rowId);
            if (Arrays.compare(key, keyValue[0]) <= 0 || rowId == nodePage.get_nextRowId() - 1) {
                break;
            }
            leftPage = keyValue[1];
        }
        bm.unpinPage(pageId, fileTitle);
        return ByteBuffer.wrap(leftPage).getInt();
    }

    private int[] getSearchPath(byte[] key) {
        return null;
    }

}
