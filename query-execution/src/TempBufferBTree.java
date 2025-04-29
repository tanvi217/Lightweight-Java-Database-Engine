// if the file title "BTree" is not accessed by any other caller, then we could add some BTree id 0,1,2... to the file name to make sure different instance of BTree use different files
// should the key length be set in the constructor somehow? (so not fixed)

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class TempBufferBTree<K extends Comparable<K>> implements BTree<K> {

    private static int numInstances = 0; // used for creating a unique fileTitle for each new BTree
    private BufferManager bm;
    
    private int rootPid; // page id of root
    private int treeDepth; // number of layers in tree. So single leaf node as root has treeDepth 1
    private int bytesInKey; // number of bytes in search key
    private String fileTitle;
    private boolean debugPrinting;
    private int[] keyRange;
    private int[] ridRange;
    private int[] firstInt;
    private int[] secondInt;

    public TempBufferBTree(BufferManager bm, int bytesInKey, boolean debugPrinting) { //.
        this.bm = bm;
        this.bytesInKey = bytesInKey;
        keyRange = new int[] {0, bytesInKey};
        ridRange = new int[] {bytesInKey, bytesInKey + 8};
        firstInt = new int[] {bytesInKey, bytesInKey + 4};
        secondInt = new int[] {bytesInKey + 4, bytesInKey + 8};
        this.debugPrinting = debugPrinting;
        treeDepth = 0;
        fileTitle = "bm-B-plus-tree-" + (++numInstances);
        createNewRoot(); // sets treeDepth to 1 and rootPid to correct page ID
    }

    public TempBufferBTree(BufferManager bm, int bytesInKey) { this(bm, bytesInKey, false); }

    @Override
    public Iterator<Rid> search(K callerKey) { //.
        byte[] key = getKeyFromComparable(callerKey); // convert from int, String, etc.. to byte[]
        return internalRangeSearch(key, key);
    }

    @Override
    public Iterator<Rid> rangeSearch(K callerStartKey, K callerEndKey) { //.
        byte[] startKey = getKeyFromComparable(callerStartKey);
        byte[] endKey = getKeyFromComparable(callerEndKey);
        return internalRangeSearch(startKey, endKey);
    }

    @Override
    public void insert(K callerKey, Rid rid) { //.
        byte[] key = getKeyFromComparable(callerKey);
        int[] searchPath = getSearchPath(key);
        Row newRow = createRow(key, rid.getPageId(), rid.getSlotId());
        insertAlongPath(newRow, searchPath, treeDepth - 1);
    }

    @Override
    public String toString() { //.
        return fileTitle + " depth is " + treeDepth; 
    }

    // sets the rootPid and treeDepth instance variables
    private void createNewRoot() { //.
        boolean isLeaf = treeDepth == 0; // very first root, new node will be leaf
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4); // leaf nodes have two ints
        Page root = createPage(bytesInRow);
        rootPid = root.getId();
        if (isLeaf) {
            setLeafSiblings(root, -1, -1);
        }
        unpinPage(rootPid);
        ++treeDepth;
    }

    private Row createRow(Object key, int... data) { //.
        ByteBuffer dataBuffer = ByteBuffer.allocate(bytesInKey + data.length * 4);
        if (key == null) {
            dataBuffer.position(bytesInKey);
        } else if (key instanceof ByteBuffer) {
            dataBuffer.put((ByteBuffer) key);
        } else { // key is byte[]
            dataBuffer.put((byte[]) key);
        }
        for (int attr : data) {
            dataBuffer.putInt(attr);
        }
        return new Row(dataBuffer);
    }
    
    // updates the two integer page IDs stored in the first row of a leaf node
    private void setLeafSiblings(Page leaf, int leftSibPid, int rightSibPid) { //.
        Row siblingRow = createRow(null, leftSibPid, rightSibPid);
        if (leaf.height() == 0) { // if leaf is empty, function to insert into first row is different
            leaf.insertRow(siblingRow);
        } else {
            leaf.overwriteRow(siblingRow, 0);
        }
        markDirty(leaf.getId());
    }

    // converts strings and integers to byte arrays. If K is neither, use the integer callerKey.hashCode()
    private byte[] getKeyFromComparable(K callerKey) { //.
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
    private Iterator<Rid> internalRangeSearch(byte[] startKey, byte[] endKey) { //.
        int leafPid = getSearchPath(startKey)[treeDepth - 1]; // leaf node is last in search path
        return getLeafMatches(leafPid, startKey, endKey).iterator();
    }

    // starts at root, and follows pointers in the tree to the leaf page containing the first occurence of the key
    private int[] getSearchPath(byte[] key) { //.
        int[] searchPath = new int[treeDepth];
        int currentPid = rootPid;
        int depthIndex = 0;
        while (depthIndex < treeDepth - 1) { // depthIndex is less than the depth index of the leaves
            searchPath[depthIndex] = currentPid;
            // currentPid is definitely not the ID of a leaf
            currentPid = firstMatchingRow(key, currentPid).getInt(firstInt);
            ++depthIndex;
        }
        searchPath[depthIndex] = currentPid; // if treeDepth == 1 and root is a leaf, this will be rootPid
        return searchPath;
    }

    private Page getPage(int pid) { //.
        return bm.getPage(pid, fileTitle);
    }

    private void unpinPage(int pid) { //.
        bm.unpinPage(pid, fileTitle);
    }

    private void markDirty(int pid) { //.
        bm.markDirty(pid, fileTitle);
    }

    private Page createPage(int bytesInRow) { //.
        return bm.createPage(fileTitle, bytesInRow);
    }

    // the row with id 0 only begins with a meaningful key when 'node' is a leaf
    private Row firstMatchingRow(byte[] key, int pid) { //.
        Page node = getPage(pid);
        if (node.height() < 1) {
            throw new IllegalStateException("Node must have at least one row to match with.");
        }
        int rowId = 0;
        int comparison = 1; // The first row is assumed to be less than the key
        while (rowId < node.height() - 1 && comparison > 0) { // increment rowId until the end of the node or we find a row with key <= given key
            ++rowId;
            comparison = compareKeys(key, node.getRow(rowId));
        }
        if (comparison < 0) { // given key is less than node.getRow(rowId)
            --rowId;
        } // otherwise, given key exactly matches node.getRow(rowId), or the given key is greater than the last row in the node
        Row match = node.getRow(rowId);
        unpinPage(pid);
        return match;
    }

    private List<Rid> getLeafMatches(int leafPid, byte[] startKey, byte[] endKey) { //.
        List<Rid> matches = new ArrayList<>();
        Page leaf;

        try {
            leaf = getPage(leafPid);
        } catch (Exception e) {
            System.out.println("Unexpected error, invalid pid passed to getLeafMatches.");
            return matches;
        }
        
        int rowId = 1; // start at second row
        while (rowId < leaf.height() && compareKeys(startKey, leaf.getRow(rowId)) > 0) {
            ++rowId;
        }
        if (debugPrinting) {
            System.out.println("First match found at row " + rowId + " of page " + leafPid);
        }
        while (rowId < leaf.height() && compareKeys(endKey, leaf.getRow(rowId)) >= 0) {
            matches.add(new Rid(leaf.getRow(rowId).getRange(ridRange))); // passes ByteBuffer of Rid data directly to constructor
            ++rowId;
        }
        if (debugPrinting) {
            System.out.println(" Last match found at row " + (rowId - 1) + " of page " + leafPid);
            System.out.println(" - Total number of rows was: " + leaf.height());
        }
        // if last row still matches (i.e. rowId == leaf.height()), some matches may be in next leaf

        int nextLeafPid = (rowId == leaf.height()) ? leaf.getRow(0).getInt(secondInt) : -2; // if value from row is -1 then we have reached the end of leaf pages. -2 if we did finish searching and don't need a next page ID
        unpinPage(leafPid);
        if (nextLeafPid >= 0) {
            matches.addAll(getLeafMatches(nextLeafPid, startKey, endKey));
        }
        return matches;
    }

    private void insertAlongPath(Row newRow, int[] searchPath, int depthIndex) { //.
        int targetPid = searchPath[depthIndex];
        Page target = getPage(targetPid);
        if (target.isFull()) {
            unpinPage(targetPid);
            splitAndInsertAlongPath(newRow, searchPath, depthIndex);
            return;
        }
        insertIntoOpenNode(newRow, target);
        unpinPage(targetPid);
    }

    private void splitAndInsertAlongPath(Row newRow, int[] searchPath, int depthIndex) { //.
        int targetPid = searchPath[depthIndex];
        Page target = getPage(targetPid);
        if (debugPrinting && depthIndex < treeDepth - 1) {
            System.out.println("Splitting Non-Leaf Node: " + target);
            for (int i = 0; i < treeDepth; ++i) {
                System.out.print(searchPath[i] + ", ");
            }
            System.out.println("<- Search Path Page IDs (leftmost is root)");
        }
        int middleRowId = target.height() / 2; // this is the number of rows currently in the page integer divided by 2
        Row middleRow = target.getRow(middleRowId);
        int siblingPid = createNewSibling(targetPid, depthIndex);
        //a little optimization which ISN'T implemented could be check if the key to be inserted is in the second half of the page
        //i.e. it is in the new page, if that is the case then we could add it in appropriately in the below loop.
        int comparison = compareKeys(newRow, target.getRow(middleRowId)); // key to middleKey
        //may want to change to >, but i think this is correct
        /*if(comparison>=0){
            //now we continue on with optimized portion, a lot of copy and pasting
        }*/
        Page sibling = getPage(siblingPid);
    
        //we KNOW that comparison <0 --> key < middleKey --> where to insert key must be in
        //here we do things as normal
        //first we fill the newPage appropriately
        int rowIdInTarget = middleRowId;
        while(rowIdInTarget < target.height()){
            sibling.insertRow(target.getRow(rowIdInTarget));
            ++rowIdInTarget;
        }
        markDirty(siblingPid);
        
        //now the new page is filled properly, and we insert the key into the appropriate spot in pageToSplit aka oldPage
        //we also need to "delete" the moved entries from pageToSplit which is the same as setting newRowId appropriately
        //don't need +1 because middleKey in right page
        target.setHeight(middleRowId);
        markDirty(targetPid);
        if (comparison < 0) {
            insertIntoOpenNode(newRow, target);
        } else {
            insertIntoOpenNode(newRow, sibling);
        }
        unpinPage(targetPid);
        unpinPage(siblingPid);
        Row pointerRow = createRow(middleRow.getRange(keyRange), siblingPid);

        if (depthIndex == 0) { // splitting the root
            createNewRoot(); // updates treeDepth and sets rootPid
            Page root = getPage(rootPid);
            Row leftPointerRow = createRow(null, targetPid);
            root.insertRow(leftPointerRow);
            root.insertRow(pointerRow);
            markDirty(rootPid);
            unpinPage(rootPid);
        } else {
            insertAlongPath(pointerRow, searchPath, depthIndex - 1); // no root split, just insert into next level up the path
        }
    }

    // returns page ID of new sibling
    private int createNewSibling(int leftPid, int depthIndex) { //.
        boolean isLeaf = depthIndex == treeDepth - 1; 
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4); // leaf nodes have two ints
        Page right = createPage(bytesInRow);
        int rightPid = right.getId();
        if (isLeaf) {
            Page left = getPage(leftPid);
            int farLeftPid = left.getRow(0).getInt(firstInt); // original left pointer of left node
            int farRightPid = left.getRow(0).getInt(secondInt); // original right pointer of left node
            setLeafSiblings(left, farLeftPid, rightPid);
            setLeafSiblings(right, leftPid, farRightPid);
            unpinPage(leftPid);
        }
        unpinPage(rightPid);
        return rightPid;
    }

    private int compareKeys(byte[] keyA, Row keyRowB) { //.
        return -keyRowB.compareTo(keyA, keyRange);
    }

    private int compareKeys(Row keyRowA, Row keyRowB) { //.
        return keyRowA.getRange(keyRange).compareTo(keyRowB.getRange(keyRange));
    }

    // relies on the fact that there is space in the target node
    private void insertIntoOpenNode(Row newRow, Page target) {
        int rowId = 1;
        while (rowId < target.height()) {
            if (compareKeys(newRow, target.getRow(rowId)) <= 0) {
                break;
            }
            ++rowId;
        }
        target.insertRow(newRow, rowId); // insertRow with rowId argument shifts rows to make space
        markDirty(target.getId());
    }

}
