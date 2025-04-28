// if the file title "BTree" is not accessed by any other caller, then we could add some BTree id 0,1,2... to the file name to make sure different instance of BTree use different files
// should the key length be set in the constructor somehow? (so not fixed)

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class BufferBTree<K extends Comparable<K>> implements BTree<K> {

    private static int numInstances = 0; // used for creating a unique fileTitle for each new BTree
    private BufferManager bm;
    
    private int rootPid; // page id of root
    private int treeDepth; // number of layers in tree. So single leaf node as root has treeDepth 1
    private int bytesInKey; // number of bytes in search key
    private String fileTitle;
    private int pinDepth; // unused for now
    private boolean debugPrinting;

    public BufferBTree(BufferManager bm, int bytesInKey, int pinDepth, boolean debugPrinting) {
        this.bm = bm;
        this.bytesInKey = bytesInKey;
        this.pinDepth = pinDepth;
        this.debugPrinting = debugPrinting;
        treeDepth = 0;
        fileTitle = "bm-B-plus-tree-" + (++numInstances);
        createNewRoot(); // sets treeDepth to 1 and rootPid to correct page ID
    }

    public BufferBTree(BufferManager bm, int bytesInKey, int pinDepth) { this(bm, bytesInKey, pinDepth, false); }
    public BufferBTree(BufferManager bm, int bytesInKey) { this(bm, bytesInKey, 0, false); }

    @Override
    public Iterator<Rid> search(K callerKey) {
        byte[] key = getKeyFromComparable(callerKey); // convert from int, String, etc.. to byte[]
        return internalRangeSearch(key, key);
    }

    @Override
    public Iterator<Rid> rangeSearch(K callerStartKey, K callerEndKey) {
        byte[] startKey = getKeyFromComparable(callerStartKey);
        byte[] endKey = getKeyFromComparable(callerEndKey);
        return internalRangeSearch(startKey, endKey);
    }

    @Override
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

    // sets the rootPid and treeDepth instance variables
    private void createNewRoot() {
        boolean isLeaf = treeDepth == 0; // very first root, new node will be leaf
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4); // leaf nodes have two ints
        rootPid = bm.createPage(fileTitle, bytesInRow).getId();
        if (isLeaf) {
            setLeafSiblings(rootPid, -1, -1);
        }
        bm.unpinPage(rootPid, fileTitle);
        ++treeDepth;
    }
    
    // updates the two integer page IDs stored in the first row of a leaf node
    private void setLeafSiblings(int leafPid, int leftSibPid, int rightSibPid) {
        ByteBuffer sibPids = ByteBuffer.allocate(8);
        sibPids.putInt(leftSibPid);
        sibPids.putInt(rightSibPid);
        Row siblingRow = new Row(sibPids.array());
        Page leaf = bm.getPage(leafPid, fileTitle);
        if (leaf.height() == 0) { // if leaf is empty, function to insert into first row is different
            leaf.insertRow(siblingRow);
        } else {
            leaf.overwriteRow(siblingRow, 0);
        }
        bm.markDirty(leafPid, fileTitle);
        bm.unpinPage(leafPid, fileTitle);
    }

    // converts strings and integers to byte arrays. If K is neither, use the integer callerKey.hashCode()
    private byte[] getKeyFromComparable(K callerKey) {
        if (callerKey instanceof String) {
            String keyString = ((String) callerKey).toLowerCase();
            return Arrays.copyOf(keyString.getBytes(StandardCharsets.UTF_8), bytesInKey);
        }
        if (callerKey instanceof Integer) {
            String inBase36 = Integer.toString((Integer) callerKey, 36);
            String atLength = String.format("%" + bytesInKey + "s", inBase36).replace(' ', '0');
            return Arrays.copyOf(atLength.getBytes(StandardCharsets.UTF_8), bytesInKey);
        }
        if (bytesInKey != 4) {
            throw new IllegalArgumentException("If the key is not a string or an integer, then the number of bytes in a key must be 4.");
        }
        return ByteBuffer.allocate(4).putInt(callerKey.hashCode()).array();
    }

    // finds the search path to the node containing the start key, and finds matches starting there
    private Iterator<Rid> internalRangeSearch(byte[] startKey, byte[] endKey) {
        int leafPid = getSearchPath(startKey)[treeDepth - 1]; // leaf node is last in search path
        return getLeafMatches(leafPid, startKey, endKey).iterator();
    }

    // starts at root, and follows pointers in the tree to the leaf page containing the first occurence of the key
    private int[] getSearchPath(byte[] key) {
        int[] searchPath = new int[treeDepth];
        int currentPid = rootPid;
        int depthIndex = 0;
        while (depthIndex < treeDepth - 1) { // depthIndex is less than the depth index of the leaves
            searchPath[depthIndex] = currentPid;
            currentPid = findInNonLeafPage(key, currentPid); // currentPid is definitely not the ID of a leaf
            ++depthIndex;
        }
        searchPath[depthIndex] = currentPid; // if treeDepth == 1 and root is a leaf, this will be rootPid
        return searchPath;
    }

    // finds the page ID pointer which is followed when searching for the given key in this index
    private int findInNonLeafPage(byte[] key, int nodePid) {
        Page node = bm.getPage(nodePid, fileTitle);
        byte[] leftPointer = dataAfterKey(node, 0); // page ID bytes of first row
        int rowId = 1; // start loop at second row
        while (rowId < node.height()) {
            if (compareKeyToRow(key, node, rowId) <= 0) { // compare key to current row's key
                break;
            }
            leftPointer = dataAfterKey(node, rowId); // page ID bytes of current row
            ++rowId;
        } // if break is never reached, the loop ends with leftPointer being the page ID bytes of the final row
        bm.unpinPage(nodePid, fileTitle);
        return ByteBuffer.wrap(leftPointer).getInt(); // convert page ID bytes to int
    }

    // gets everything in a row's data array except for the bytes corresponding to the key
    private byte[] dataAfterKey(Page node, int rowId) {
        byte[] rowData = node.getRow(rowId).data;
        return Arrays.copyOfRange(rowData, bytesInKey, rowData.length);
    }

    // compares the key part of the given byte[] with the key part of the specified row
    private int compareKeyToRow(byte[] key, Page node, int rowId) {
        byte[] rowData = node.getRow(rowId).data;
        return Arrays.compare(key, 0, bytesInKey, rowData, 0, bytesInKey);
    }

    // if row is an array of ints, gets the int at the specified index
    private int getIntFromRow(Page node, int rowId, int index) {
        byte[] rowData = node.getRow(rowId).data;
        return ByteBuffer.wrap(rowData).getInt(index * 4);
    }

    private List<Rid> getLeafMatches(int leafPid, byte[] startKey, byte[] endKey) {
        List<Rid> matches = new ArrayList<>();
        Page leaf = null;

        try {
            leaf = bm.getPage(leafPid, fileTitle);
        } catch (Exception e) {
            return matches;
        }
        
        int rowId = 1; // start at second row
        while (rowId < leaf.height() && compareKeyToRow(startKey, leaf, rowId) > 0) {
            ++rowId;
        }
        if (debugPrinting) {
            System.out.println("First match found at row " + rowId + " of page " + leafPid);
        }
        while (rowId < leaf.height() && compareKeyToRow(endKey, leaf, rowId) >= 0) {
            matches.add(new Rid(dataAfterKey(leaf, rowId))); // passes byte array of Rid data directly to constructor
            ++rowId;
        }
        if (debugPrinting) {
            System.out.println(" Last match found at row " + (rowId - 1) + " of page " + leafPid);
            System.out.println(" - Total number of rows was: " + leaf.height());
        }
        // if last row still matches (i.e. rowId == leaf.height()), some matches may be in next leaf

        int nextLeafPid = rowId == leaf.height() ? getIntFromRow(leaf, 0, 1) : -2; // if value from row is -1 then we have reached the end of leaf pages. -2 if we did finish searching and don't need a next page ID
        bm.unpinPage(leafPid, fileTitle);
        if (nextLeafPid >= 0) {
            matches.addAll(getLeafMatches(nextLeafPid, startKey, endKey));
        }
        return matches;
    }

    private void insertAlongPath(byte[] key, Row newRow, int[] searchPath, int depthIndex) {
        int targetPid = searchPath[depthIndex];
        Page target = bm.getPage(targetPid, fileTitle);
        if (target.isFull()) {
            bm.unpinPage(targetPid, fileTitle);
            splitAndInsertAlongPath(key, newRow, searchPath, depthIndex);
            return;
        }
        insertIntoOpenNode(key, newRow, target);
        bm.unpinPage(targetPid, fileTitle);
    }

    private void splitAndInsertAlongPath(byte[] key, Row newRow, int[] searchPath, int depthIndex) {
        int targetPid = searchPath[depthIndex];
        Page target = bm.getPage(targetPid, fileTitle);
        if (debugPrinting && depthIndex < treeDepth - 1) {
            System.out.println("Splitting Non-Leaf Node: " + target);
            for (int i = 0; i < treeDepth; ++i) {
                System.out.print(searchPath[i] + ", ");
            }
            System.out.println("<- Search Path Page IDs (leftmost is root)");
        }
        int middleRowId = target.height() / 2; // this is the number of rows currently in the page divided by 2
        byte[] middleRowData = target.getRow(middleRowId).data;
        byte[] middleKey = Arrays.copyOf(middleRowData, bytesInKey); // save the middleKey itself to use later
        int siblingPid = createNewSibling(targetPid, depthIndex);
        //a little optimization which ISN'T implemented could be check if the key to be inserted is in the second half of the page
        //i.e. it is in the new page, if that is the case then we could add it in appropriately in the below loop.
        int comparison = compareKeyToRow(key, target, middleRowId); // key to middleKey
        //may want to change to >, but i think this is correct
        /*if(comparison>=0){
            //now we continue on with optimized portion, a lot of copy and pasting
        }*/
        Page sibling = bm.getPage(siblingPid, fileTitle);
    
        //we KNOW that comparison <0 --> key < middleKey --> where to insert key must be in
        //here we do things as normal
        //first we fill the newPage appropriately
        int rowIdInTarget = middleRowId;
        while(rowIdInTarget < target.height()){
            sibling.insertRow(target.getRow(rowIdInTarget));
            ++rowIdInTarget;
        }
        bm.markDirty(siblingPid, fileTitle);
        
        //now the new page is filled properly, and we insert the key into the appropriate spot in pageToSplit aka oldPage
        //we also need to "delete" the moved entries from pageToSplit which is the same as setting newRowId appropriately
        //don't need +1 because middleKey in right page
        target.setHeight(middleRowId);
        bm.markDirty(targetPid, fileTitle);
        if (comparison < 0) {
            insertIntoOpenNode(key, newRow, target);
        } else {
            insertIntoOpenNode(key, newRow, sibling);
        }
        bm.unpinPage(targetPid, fileTitle);
        bm.unpinPage(siblingPid, fileTitle);
        ByteBuffer pointerRowData = ByteBuffer.allocate(bytesInKey + 4); // will be inserted into non-leaf parent
        pointerRowData.put(middleKey);
        pointerRowData.putInt(siblingPid);
        Row pointerRow = new Row(pointerRowData.array());

        if (depthIndex == 0) { // splitting the root
            createNewRoot(); // updates treeDepth and sets rootPid
            Page root = bm.getPage(rootPid, fileTitle);
            ByteBuffer leftPointerRowData = ByteBuffer.allocate(bytesInKey + 4);
            leftPointerRowData.position(bytesInKey);
            leftPointerRowData.putInt(targetPid);
            Row leftPointerRow = new Row(leftPointerRowData.array());
            root.insertRow(leftPointerRow);
            root.insertRow(pointerRow);
            bm.markDirty(rootPid, fileTitle);
            bm.unpinPage(rootPid, fileTitle);
        } else {
            insertAlongPath(middleKey, pointerRow, searchPath, depthIndex - 1); // no root split, just insert into next level up the path
        }
    }

    // returns page ID of new sibling
    private int createNewSibling(int leftPid, int depthIndex) {
        boolean isLeaf = depthIndex == treeDepth - 1; 
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4); // leaf nodes have two ints
        Page right = bm.createPage(fileTitle, bytesInRow);
        int rightPid = right.getId();
        if (isLeaf) {
            Page left = bm.getPage(leftPid, fileTitle);
            int farLeftPid = getIntFromRow(left, 0, 0); // original left pointer of left node
            int farRightPid = getIntFromRow(left, 0, 1); // original right pointer of left node
            setLeafSiblings(leftPid, farLeftPid, rightPid);
            setLeafSiblings(rightPid, leftPid, farRightPid);
            bm.unpinPage(leftPid, fileTitle);
        }
        bm.unpinPage(rightPid, fileTitle);
        return rightPid;
    }
    
    private void insertIntoOpenNode(byte[] key, Row newRow, Page target) {
        int rowId = 1;
        while (rowId < target.height()) {
            if (compareKeyToRow(key, target, rowId) <= 0) {
                break;
            }
            ++rowId;
        }
        Row rowToInsert = newRow;
        while (rowId < target.height()) {
            Row rowToMove = target.getRow(rowId);
            target.overwriteRow(rowToInsert, rowId);
            rowToInsert = rowToMove;
            ++rowId;
        }
        target.insertRow(rowToInsert);
        bm.markDirty(target.getId(), fileTitle);
    }

}
