// if the file title "BTree" is not accessed by any other caller, then we could add some BTree id 0,1,2... to the file name to make sure different instance of BTree use different files
// should the key length be set in the constructor somehow? (so not fixed)

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class MRTempFile<K> {

    private static int numInstances = 0; // used for creating a unique fileTitle for each new BTree
    private BufferManager bm;
    
    private int rootPid; // page id of root
    private int treeDepth; // number of layers in tree. So single leaf node as root has treeDepth 1
    private int bytesInKey; // number of bytes in search key
    private String fileTitle;
    private int pinDepth; // unused for now

    // updates the two integers stored in the 
    private void setLeafPointerRow(int targetPageId, int leftPageId, int rightPageId) {
        ByteBuffer twoInts = ByteBuffer.allocate(8);
        twoInts.putInt(leftPageId);
        twoInts.putInt(rightPageId);
        Row newRow = new Row(twoInts.array());
        Page leafPage = bm.getPage(targetPageId, fileTitle);
        if (leafPage.height() == 0) {
            leafPage.insertRow(newRow);
        } else {
            leafPage.modifyRow(newRow, 0);
        }
        bm.unpinPage(targetPageId, fileTitle);
    }

    private int createNewRoot() {
        boolean isLeaf = treeDepth == 0; // very first root, new node will be leaf
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4); // leaf nodes have two ints
        int pageId = bm.createPage(fileTitle, bytesInRow).getId();
        if (isLeaf) {
            setLeafPointerRow(pageId, -1, -1);
        }
        bm.unpinPage(pageId, fileTitle);
        ++treeDepth;
        return pageId;
    }

    public MRTempFile(BufferManager bm, int bytesInKey, int pinDepth, boolean debugPrinting) {
        this.bm = bm;
        this.bytesInKey = bytesInKey;
        this.pinDepth = pinDepth;
        treeDepth = 0;
        fileTitle = "B-plus-tree-" + (++numInstances);
        rootPid = createNewRoot(); // sets treeDepth to 1
    }

    public MRTempFile(BufferManager bm, int bytesInKey, int pinDepth) { this(bm, bytesInKey, pinDepth, false); }
    public MRTempFile(BufferManager bm, int bytesInKey) { this(bm, bytesInKey, 0, false); }

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
        }
        if (callerKey instanceof Integer) {
            int keyInt = (Integer) callerKey;
            return ByteBuffer.allocate(4).putInt(keyInt).array();
        }
        return ByteBuffer.allocate(4).putInt(callerKey.hashCode()).array();
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
        while (rowId < pageHeight && compareKeyToRow(startKey, leafPage, rowId) > 0) {
            ++rowId;
        }
        while (rowId < pageHeight && compareKeyToRow(endKey, leafPage, rowId) >= 0) {
            matches.add(new Rid(dataAfterKey(leafPage, rowId)));
            ++rowId;
        }
        boolean unfinished = rowId == pageHeight;
        int nextLeafPid = unfinished ? getIntFromRow(leafPage, 0, 1) : -1; // if value from row is -1 then we have reached the end of leaf pages
        bm.unpinPage(leafPid, fileTitle);
        if (nextLeafPid > 0) { // if last row still matches, some matches may be in next leaf
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

    private int createNewSibling(int leftPageId, int depthIndex) {
        boolean isLeaf = depthIndex == treeDepth - 1; 
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4); // leaf nodes have two ints
        Page rightNode = bm.createPage(fileTitle, bytesInRow);
        int rightPageId = rightNode.getId();
        if (isLeaf) {
            Page leftNode = bm.getPage(leftPageId, fileTitle);
            int farLeftPageId = getIntFromRow(leftNode, 0, 0);
            int farRightPageId = getIntFromRow(leftNode, 0, 1);
            setLeafPointerRow(leftPageId, farLeftPageId, rightPageId);
            setLeafPointerRow(rightPageId, leftPageId, farRightPageId);
            bm.unpinPage(leftPageId, fileTitle);
        }
        bm.unpinPage(rightPageId, fileTitle);
        return rightPageId;
    }

    // private void splitAndInsertAlongPath(byte[] key, Row newRow, int[] searchPath, int depthIndex) {
    //     int nodePageId = searchPath[depthIndex];
    //     Page targetPage = bm.getPage(nodePageId, fileTitle);
    //     int siblingPid = createNewSibling(nodePageId, depthIndex);

    //     if (depthIndex == 0) { // splitting the root
    //         int parentPageId = createNewRoot(); // updates treeDepth

    //         rootPid = parentPageId;
    //         return;
    //     }
    //     int parentPageId = searchPath[depthIndex - 1];

    //     // insert pointers into parent
    // }

    private void insertIntoOpenNode(byte[] key, Row newRow, Page targetPage) {
        int rowId = 1;
        while (rowId < targetPage.height()) {
            if (compareKeyToRow(key, targetPage, rowId) <= 0) {
                break;
            }
            ++rowId;
        }
        Row rowToInsert = newRow;
        while (rowId < targetPage.height()) {
            Row rowToMove = targetPage.getRow(rowId);
            targetPage.modifyRow(rowToInsert, rowId);
            rowToInsert = rowToMove;
            ++rowId;
        }
        targetPage.insertRow(rowToInsert);
    }

    private void splitAndInsertAlongPath(byte[] key, Row newRow, int[] searchPath, int depthIndex) {
        int nodePageId = searchPath[depthIndex];
        Page targetPage = bm.getPage(nodePageId, fileTitle);
        int middleRowId = targetPage.height() / 2; //this is the number of rows currently in the page divided by 2
        //save the middleKey itself to use later
        byte[] middleRowData = targetPage.getRow(middleRowId).data;
        byte[] middleKey = Arrays.copyOf(middleRowData, bytesInKey);
        int rowIdInTarget = middleRowId;
        //creation of new page
        boolean isLeaf = nodePageId == searchPath[treeDepth - 1];
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4);
        int siblingPid = createNewSibling(nodePageId, depthIndex);
        //a little optimization which ISN'T implemented could be check if the key to be inserted is in the second half of the page
        //i.e. it is in the new page, if that is the case then we could add it in appropriately in the below loop.
        int comparison = compareKeyToRow(key, targetPage, middleRowId); // key to middleKey
        //may want to change to >, but i think this is correct
        /*if(comparison>=0){
            //now we continue on with optimized portion, a lot of copy and pasting
        }*/
        Page siblingNode = bm.getPage(siblingPid, fileTitle);
    
        //we KNOW that comparison <0 --> key < middleKey --> where to insert key must be in
        //here we do things as normal
        //first we fill the newPage appropriately
        while(rowIdInTarget < targetPage.height()){
            siblingNode.insertRow(targetPage.getRow(rowIdInTarget));
            ++rowIdInTarget;
        }
        
        //now the new page is filled properly, and we insert the key into the appropriate spot in pageToSplit aka oldPage
        //we also need to "delete" the moved entries from pageToSplit which is the same as setting newRowId appropriately
        //don't need +1 because middleKey in right page
        targetPage.setHeight(middleRowId);
        if (comparison < 0) {
            insertIntoOpenNode(key, newRow, targetPage);
        } else {
            insertIntoOpenNode(key, newRow, siblingNode);
        }
        bm.unpinPage(targetPage.getId(), fileTitle);
        bm.unpinPage(siblingNode.getId(), fileTitle);
        ByteBuffer pointerRowData = ByteBuffer.allocate(bytesInKey + 4);
        pointerRowData.put(middleKey);
        pointerRowData.putInt(siblingPid);
        Row pointerRow = new Row(pointerRowData.array());

        if (depthIndex == 0) { // splitting the root
            int parentPageId = createNewRoot(); // updates treeDepth
            int nonLeafNodeRowLength = bytesInKey + 4;
            Page newRoot = bm.getPage(parentPageId, fileTitle);
            ByteBuffer leftPointerRowData = ByteBuffer.allocate(nonLeafNodeRowLength);
            leftPointerRowData.position(bytesInKey);
            leftPointerRowData.putInt(nodePageId);
            Row leftPointerRow = new Row(pointerRowData.array());
            newRoot.insertRow(leftPointerRow);
            newRoot.insertRow(pointerRow);
            bm.unpinPage(parentPageId, fileTitle);
            rootPid = parentPageId;
        } else {
            insertAlongPath(middleKey, pointerRow, searchPath, depthIndex - 1);
        }
    }

    private void insertAlongPath(byte[] key, Row newRow, int[] searchPath, int depthIndex) {
        int nodePageId = searchPath[depthIndex];
        Page targetPage = bm.getPage(nodePageId, fileTitle);
        if (targetPage.isFull()) {
            bm.unpinPage(targetPage.getId(), fileTitle);
            splitAndInsertAlongPath(key, newRow, searchPath, depthIndex);
            return;
        }
        insertIntoOpenNode(key, newRow, targetPage);
        bm.unpinPage(targetPage.getId(), fileTitle);
    }

    public void insert(K callerKey, Rid rid) {
        byte[] key = getKeyFromComparable(callerKey);
        int[] searchPath = getSearchPath(key);
        ByteBuffer rowData = ByteBuffer.allocate(bytesInKey + 8);
        rowData.put(key);
        rowData.putInt(rid.getPageId());
        rowData.putInt(rid.getSlotId());
        insertAlongPath(key, new Row(rowData.array()), searchPath, treeDepth - 1);
    }

    @Override
    public String toString() {
        return fileTitle + " depth is " + treeDepth; 
    }

}
