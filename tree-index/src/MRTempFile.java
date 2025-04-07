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
    private int pinDepth;

    private int createNodePage(int depthIndex) {
        boolean isLeaf = depthIndex == treeDepth - 1;
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4); // leaf nodes have two ints
        int pageId = bm.createPage(fileTitle, bytesInRow).getId();
        if (depthIndex >= pinDepth) { // if depthIndex < pinDepth keep page pinned
            bm.unpinPage(pageId, fileTitle);
        }
        return pageId;
    }

    public MRTempFile(BufferManager bm, int bytesInKey, int pinDepth) {
        this.bm = bm;
        this.bytesInKey = bytesInKey;
        this.pinDepth = pinDepth;
        treeDepth = 1;
        fileTitle = "B-plus-tree-" + (++numInstances);
        rootPid = createNodePage(0);
    }

    public MRTempFile(BufferManager bm, int bytesInKey) { this(bm, bytesInKey, 0); }

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

    private void handleSplit(byte[] key, Row newRow, int nodePageId, int[] searchPath) {
        Page targetPage = bm.getPage(nodePageId, fileTitle);
        int middleRowId = targetPage.height() / 2; //this is the number of rows currently in the page divided by 2
        //save the middleKey itself to use later
        byte[] middleRowData = targetPage.getRow(middleRowId).data;
        int currOldPageRowId = middleRowId;
        //creation of new page
        boolean isLeaf = nodePageId == searchPath[treeDepth - 1];
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4);
        Page newPage = bm.createPage(fileTitle, bytesInRow);
        int newPageId = newPage.getId();
        //a little optimization which ISN'T implemented could be check if the key to be inserted is in the second half of the page
        //i.e. it is in the new page, if that is the case then we could add it in appropriately in the below loop.
        int comparison = compareKeyToRow(key, targetPage, middleRowId); // key to middleKey
        //may want to change to >, but i think this is correct
        /*if(comparison>=0){
            //now we continue on with optimized portion, a lot of copy and pasting
        }*/
    
        //we KNOW that comparison <0 --> key < middleKey --> where to insert key must be in
        //here we do things as normal
        //first we fill the newPage appropriately
        while(currOldPageRowId < targetPage.height()){
            newPage.insertRow(targetPage.getRow(currOldPageRowId));
            currOldPageRowId++;
        }
        //now the new page is filled properly, and we insert the key into the appropriate spot in pageToSplit aka oldPage
        //we also need to "delete" the moved entries from pageToSplit which is the same as setting newRowId appropriately
        //don't need +1 because middleKey in right page
        targetPage.setHeight(middleRowId);
        if(isLeaf){
            insertIntoNode(key, newRow, newPage.getId(), searchPath);
            //now setting the previous and next pointers
            //first we get the original ones out
            int ogPrev = getIntFromRow(targetPage, 0, 0);
            int ogNext = getIntFromRow(targetPage, 0, 1);
            //first put the older previous and newPageId as next in the pageToSplit page
            ByteBuffer pageToSplitBuff = ByteBuffer.allocate(bytesInRow);
            pageToSplitBuff.putInt(ogPrev);
            pageToSplitBuff.putInt(newPageId);
            targetPage.modifyRow(new Row(pageToSplitBuff.array()), 0);
            
            //now put the pageToSplit as previous and the old next as the next pointer
            ByteBuffer newPageBuff = ByteBuffer.allocate(bytesInRow);
            newPageBuff.putInt(targetPage.getId());
            newPageBuff.putInt(ogNext);
            newPage.modifyRow(new Row(newPageBuff.array()), 0);

            //now previous and next should be properly created/modified.
            //if we have an internal/nonleaf node we do NOT need to worry about this process.
        }
        else{
            //insertIntoNonLeafPage(key, rid.getPageId(), newPage.getId(), parentPageIds)
        }
        bm.unpinPage(targetPage.getId(), fileTitle);
        bm.unpinPage(newPage.getId(), fileTitle);
    
        //now the pages are correctly created, we just need to "push up" the right value to insertIntoNonLeafPage
        //We NEED TO CHECK IF THIS IS THE ROOT, IF IT IS THEN WE MUST CREATE A NEW NODE TO PUSH UP INTO, and can just manually do it here
        if(nodePageId == rootPid){
            //this means we are splitting the root, so need to make a new root that looks at this properly
            int nonLeafNodeRowLength = bytesInKey + 4;
            Page newRoot = bm.createPage(fileTitle, nonLeafNodeRowLength);
            //insert the first row which is just a page id, no key so...
            
            ByteBuffer newRootBuff = ByteBuffer.allocate(nonLeafNodeRowLength);
            newRootBuff.position(bytesInKey);
            newRootBuff.putInt(nodePageId);
            byte[] byteToInsert = newRootBuff.array();
            newRoot.insertRow(new Row(byteToInsert));
            //also add the second row which is the middleKeyValue alongside the newPageId
            ByteBuffer newRootBuff2 = ByteBuffer.allocate(nonLeafNodeRowLength);
            newRootBuff2.put(middleRowData, 0, bytesInKey); // first bytesInKey of middleRowData
            newRootBuff2.putInt(newPageId);
            byte[] byteToInsert2 = newRootBuff2.array();
            newRoot.insertRow(new Row(byteToInsert2));
            //now the newRoot should be setup as desired so we just need to change the rootPid so we are pointing to the correct root
            rootPid = newRoot.getId();
            //and we unpin the new page
            bm.unpinPage(newRoot.getId(), fileTitle);
        }
        //else{
        //if we aren't creating a new root then we are just adding the value to the next node up which sid one with the below function.
        //insertIntoNonLeafPage(middleKey, newPage.getId(), parentPageIds[depth], parentPageIds[]);
        //insertIntoNonLeafPage(middleKey, newPage.getId(), parentPageIds[parentPageIds.length-1], parentPageIds[]);
        //}
    }

    private void insertIntoNode(byte[] key, Row newRow, int nodePageId, int[] searchPath) {
        Page targetPage = bm.getPage(nodePageId, fileTitle);
        if (targetPage.isFull()) {
            bm.unpinPage(nodePageId, fileTitle);
            handleSplit(key, newRow, nodePageId, searchPath);
            return;
        }
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
        bm.unpinPage(targetPage.getId(), fileTitle);
    }

    public void insert(K callerKey, Rid rid) {
        byte[] key = getKeyFromComparable(callerKey);
        int[] searchPath = getSearchPath(key);
        int leafPid = searchPath[treeDepth - 1];
        ByteBuffer rowData = ByteBuffer.allocate(bytesInKey + 8);
        rowData.put(key);
        rowData.putInt(rid.getPageId());
        rowData.putInt(rid.getSlotId());
        insertIntoNode(key, new Row(rowData.array()), leafPid, searchPath);
    }

}
