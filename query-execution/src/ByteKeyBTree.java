// if the file title "BTree" is not accessed by any other caller, then we could add some BTree id 0,1,2... to the file name to make sure different instance of BTree use different files
// should the key length be set in the constructor somehow? (so not fixed)

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;    

class ByteKeyBTree<K extends Comparable<K>> implements BTree<K> {

    private static String tableTitlePrefix = "BTreeNodes";
    private static Map<Character, Character> asciiReplacements = Map.ofEntries(entry('à', 'a'), entry('á', 'a'), entry('â', 'a'), entry('ä', 'a'), entry('ǎ', 'a'), entry('æ', 'a'), entry('ã', 'a'), entry('å', 'a'), entry('ā', 'a'), entry('ç', 'c'), entry('ć', 'c'), entry('č', 'c'), entry('ċ', 'c'), entry('ď', 'd'), entry('ð', 'd'), entry('è', 'e'), entry('é', 'e'), entry('ê', 'e'), entry('ë', 'e'), entry('ě', 'e'), entry('ẽ', 'e'), entry('ē', 'e'), entry('ė', 'e'), entry('ę', 'e'), entry('ğ', 'g'), entry('ġ', 'g'), entry('ħ', 'h'), entry('ì', 'i'), entry('í', 'i'), entry('î', 'i'), entry('ï', 'i'), entry('ǐ', 'i'), entry('ĩ', 'i'), entry('ī', 'i'), entry('ı', 'i'), entry('į', 'i'), entry('ķ', 'k'), entry('ł', 'l'), entry('ļ', 'l'), entry('ľ', 'l'), entry('ñ', 'n'), entry('ń', 'n'), entry('ņ', 'n'), entry('ň', 'n'), entry('ò', 'o'), entry('ó', 'o'), entry('ô', 'o'), entry('ö', 'o'), entry('ǒ', 'o'), entry('œ', 'o'), entry('ø', 'o'), entry('õ', 'o'), entry('ō', 'o'), entry('ř', 'r'), entry('ß', 's'), entry('ş', 's'), entry('ș', 's'), entry('ś', 's'), entry('š', 's'), entry('ț', 't'), entry('ť', 't'), entry('þ', 't'), entry('ù', 'u'), entry('ú', 'u'), entry('û', 'u'), entry('ü', 'u'), entry('ǔ', 'u'), entry('ũ', 'u'), entry('ū', 'u'), entry('ű', 'u'), entry('ů', 'u'), entry('ŵ', 'w'), entry('ý', 'y'), entry('ŷ', 'y'), entry('ÿ', 'y'), entry('ź', 'z'), entry('ž', 'z'), entry('ż', 'z'));
    private String tableTitle;
    private int[] keyRange;
    private int[] firstInt;
    private int[] secondInt;
    private int rootPid; // page id of root
    private int treeDepth = 0; // number of layers in tree. So single leaf node as root has treeDepth 1
    private int bytesInKey; // number of bytes in search key
    private BufferManager bm;
    private boolean debugPrinting;
    private int[] ridRange;
    

    public ByteKeyBTree(int bytesInKey, BufferManager bm, boolean debugPrinting) {
        this.bm = bm;
        this.bytesInKey = bytesInKey;
        this.debugPrinting = debugPrinting;
        keyRange = new int[] {0, bytesInKey};
        ridRange = new int[] {bytesInKey, bytesInKey + 8};
        firstInt = new int[] {bytesInKey, bytesInKey + 4};
        secondInt = new int[] {bytesInKey + 4, bytesInKey + 8};
        tableTitle = Relation.randomizeTitle(tableTitlePrefix);
        createNewRoot(); // sets treeDepth to 1 and rootPid to correct page ID
    }

    public ByteKeyBTree(BufferManager bm, int[] stringRange) { // for use with string keys, where the stringRange is something like Movies.title and the length of the range with be bytesInKey
        this(stringRange[1] - stringRange[0], bm, false);
    }

    public ByteKeyBTree(BufferManager bm, int keyMaxAbsValue) { // for use with integer keys. If keys could range from -6 to 4, or from 3 to 6, then keyMaxAbsValue is 6
        this(Integer.toString(keyMaxAbsValue * 2, 36).length(), bm, false);
    }

    @Override
    public Iterator<Rid> search(K callerKey) { //.
        ByteBuffer key = toComparableBytes(callerKey, bytesInKey); // convert from int, String, etc.. to ByteBuffer
        return internalRangeSearch(key, key);
    }

    /**
     * Guarantees that last two elements of the result iterator have different keys.
     * This allows a next block search to use the key of the last element.
     */
    @Override
    public Iterator<Rid> groupSearch(K callerKey, int groupSize) {
        ByteBuffer key = toComparableBytes(callerKey, bytesInKey);
        return internalRangeSearch(key, key, groupSize);
    }

    @Override
    public Iterator<Rid> rangeSearch(K callerStartKey, K callerEndKey) { //.
        ByteBuffer startKey = toComparableBytes(callerStartKey, bytesInKey);
        ByteBuffer endKey = toComparableBytes(callerEndKey, bytesInKey);
        return internalRangeSearch(startKey, endKey);
    }

    @Override
    public void insert(K callerKey, Rid rid) { //.
        ByteBuffer key = toComparableBytes(callerKey, bytesInKey);
        int[] searchPath = getSearchPath(key);
        Row newRow = createRow(key, rid.getPageId(), rid.getSlotId());
        insertAlongPath(newRow, searchPath, treeDepth - 1);
    }

    @Override
    public String toString() { //.
        return tableTitle + " depth is " + treeDepth; 
    }

    // sets the rootPid and treeDepth instance variables
    private void createNewRoot() { //.
        boolean isLeaf = treeDepth == 0;
        Page root = createPage(isLeaf);
        if (isLeaf) {
            setLeafSiblings(root, -1, -1);
        }
        rootPid = root.getId();
        unpinPage(rootPid);
        ++treeDepth;
    }

    /**
     * @param key ByteBuffer which must have remaining() == bytesInKey
     * @param data Some number of ints to add to the row
     * @return A new row containing the given key, followed by the passed integers
     */
    private Row createRow(ByteBuffer key, int... data) {
        ByteBuffer dataBuffer = ByteBuffer.allocate(bytesInKey + data.length * 4);
        if (key == null) {
            dataBuffer.position(bytesInKey); // newly allocated buffer will have zeros as prefix
        } else {
            dataBuffer.put(key);
            if (dataBuffer.position() != bytesInKey) {
                throw new IllegalArgumentException("ByteBuffer passed as key is not correct length.");
            }
        }
        for (int attr : data) {
            dataBuffer.putInt(attr);
        }
        dataBuffer.clear(); // reset position to 0
        return new Row(dataBuffer);
    }
    
    /**
     * Given a leaf page, updates the two integer page IDs stored in the first row of a leaf node
     */
    private void setLeafSiblings(Page leaf, int leftSibPid, int rightSibPid) { //.
        Row siblingRow = createRow(null, leftSibPid, rightSibPid); // the integers are stored in ridRange
        if (leaf.height() == 0) { // if leaf is empty, function to insert into first row is different
            leaf.insertRow(siblingRow);
        } else {
            leaf.overwriteRow(siblingRow, 0);
        }
        markDirty(leaf.getId());
    }

    // converts strings and integers to byte buffers
    public static ByteBuffer toComparableBytes(Object input, int length) { //..
        if (input instanceof ByteBuffer) {
            ByteBuffer src = (ByteBuffer) input;
            if (src.remaining() >= length) {
                src.limit(src.position() + length);
                return src.duplicate(); // remaining should now be equal to length
            }
            ByteBuffer dst = ByteBuffer.allocate(length);
            dst.put(src);
            dst.clear();
            return dst;
        }
        if (input instanceof String) {
            String str = (String) input;
            if (str.length() > length) {
                str = str.substring(0, length);
            }
            str = str.toLowerCase();
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            if (bytes.length != str.length()) {
                str = toASCII(str);
                bytes = str.getBytes(StandardCharsets.UTF_8);
                if (bytes.length != str.length()) {
                    throw new IllegalArgumentException("ByteKeyBTree multi-byte character encoding error: " + str);
                }
            }
            return ByteBuffer.wrap(Arrays.copyOf(bytes, length));
        }
        if (input instanceof Integer) {
            int num = (int) input;
            int shift = ((int) Math.pow(36, length) - 2) / 2; // shift so base 36 string is nonnegative
            if (num > shift + 1 || num < -shift) {
                throw new IllegalArgumentException("Integer has absolute value too large to be represented with given length.");
            }
            String inBase36 = Integer.toString(num + shift, 36);
            if (inBase36.length() < length) {
                char[] leadingZeros = new char[length - inBase36.length()];
                Arrays.fill(leadingZeros, '0');
                inBase36 = new String(leadingZeros) + inBase36;
            }
            byte[] bytes = inBase36.getBytes(StandardCharsets.UTF_8);
            if (bytes.length != length) {
                throw new IllegalStateException("Unexpected error when converting integer to byte array.");
            }
            return ByteBuffer.wrap(bytes);
        }
        throw new IllegalArgumentException("Only String, integer, or ByteBuffer keys are supported in ByteKeyBTree");
    }

    public static String toASCII(String input) {
        StringBuilder sb = new StringBuilder();
        for (char c : input.toCharArray()) {
            sb.append(asciiReplacements.getOrDefault(c, c));
        }
        return sb.toString();
    }

    // finds the search path to the node containing the start key, and finds matches starting there
    private Iterator<Rid> internalRangeSearch(ByteBuffer startKey, ByteBuffer endKey, int groupSize) { //..
        int leafPid = getSearchPath(startKey)[treeDepth - 1]; // leaf node is last in search path
        return getLeafMatches(leafPid, startKey, endKey, groupSize).iterator();
    }

    private Iterator<Rid> internalRangeSearch(ByteBuffer startKey, ByteBuffer endKey) {
        return internalRangeSearch(startKey, endKey, 0);
    }

    // starts at root, and follows pointers in the tree to the leaf page containing the first occurence of the key
    private int[] getSearchPath(ByteBuffer key) { //..
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

    private Page createPage(boolean isLeaf) {
        int bytesInRow = bytesInKey + (isLeaf ? 8 : 4);
        return bm.createPage(tableTitle, bytesInRow);
    }

    private Page getPage(int pid) { return bm.getPage(pid, tableTitle); }
    private void unpinPage(int pid) { bm.unpinPage(pid, tableTitle); }
    private void markDirty(int pid) { bm.markDirty(pid, tableTitle); }

    // the row with id 0 only begins with a meaningful key when 'node' is a leaf
    public Row firstMatchingRow(ByteBuffer key, int pid) { //.
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
        Row match = node.getRow(rowId).copy();
        unpinPage(pid);
        return match;
    }

    private List<Rid> getLeafMatches(int leafPid, ByteBuffer startKey, ByteBuffer endKey, int groupSize) { //.
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
        while (rowId < leaf.height()) {
            if (groupSize == 0 && compareKeys(endKey, leaf.getRow(rowId)) < 0) {
                break; // if not doing group search then conditions below don't matter
            }
            if (compareKeys(endKey, leaf.getRow(rowId)) < 0 && matches.size() >= groupSize && rowId > 1 && compareKeys(leaf.getRow(rowId), leaf.getRow(rowId - 1)) != 0) {
                // if current row is no longer in range AND group size has been met AND this is not the first row AND last two matches contain different keys, stop adding
                break;
            }
            matches.add(new Rid(leaf.getRow(rowId).viewRange(ridRange))); // passes ByteBuffer of Rid data directly to constructor
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
            if (groupSize == 0) {
                matches.addAll(getLeafMatches(nextLeafPid, startKey, endKey, 0));
            } else {
                int nextGroupSize = Math.max(1, groupSize - matches.size());
                matches.addAll(getLeafMatches(nextLeafPid, startKey, endKey, nextGroupSize));
            }
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
        Row middleRow = target.getRow(middleRowId).copy();
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
        Row pointerRow = createRow(middleRow.viewRange(keyRange), siblingPid);

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
    private int createNewSibling(int leftPid, int depthIndex) {
        boolean isLeaf = depthIndex == treeDepth - 1; 
        Page right = createPage(isLeaf);
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

    private int compareKeys(ByteBuffer keyA, Row keyRowB) {
        if (keyA.remaining() != bytesInKey) {
            throw new IllegalArgumentException("ByteBuffer key must have the correct number of bytes.");
        }
        return keyA.compareTo(keyRowB.viewRange(keyRange));
    }

    private int compareKeys(Row keyRowA, Row keyRowB) {
        return keyRowA.viewRange(keyRange).compareTo(keyRowB.viewRange(keyRange));
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
