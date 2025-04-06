import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;

public class EvanBTreeFunctions {
    // inserts key into specific page (search as already be done)
    //we are going to store (mostly) everything as byte arrays and pageIds to make things easier
    private boolean rootIsLeaf; //true if the root node is a leaf node, false otherwise
    private int maxKeys; //max nos. of keys in node
    private BufferManager bm;
    private int rootPid = 0;
    private final int fixedKeyLength = 15;
    private int leafNodeRowLength;
    private String fileTitle;
    private int depth;
    private int nonLeafNodeRowLength;


    public EvanBTreeFunctions(BufferManager bm) {
        //each entry (root starts as a leaf node) needs a k, i.e. the actual value the index is being sorted by
        //a rid which is a page id and a slot id (an int, i.e. 4 bytes allocated for every page id and slot id should be sufficient)
        //only ONCE at the beginning/end of the page we need a previous pointer and a next pointer, both page ids
        //TEMPORARILTY FILETITLE="Btree"
        depth = 1;
        fileTitle = "BTree";
        leafNodeRowLength=fixedKeyLength+8;
        nonLeafNodeRowLength = fixedKeyLength+4;
        TabularPage root = (TabularPage)bm.createPage(fileTitle, (leafNodeRowLength));
        this.rootPid = root.getId();
        rootIsLeaf = true;
        this.bm = bm;
        //will initialize the previous and next pointers as  -1s because those are not valid pageids
        ByteBuffer b = ByteBuffer.allocate(leafNodeRowLength);
        b.putInt(-1);
        b.putInt(-1);
        byte[] byteToInsert = b.array();
        root.insertRow(new Row(byteToInsert));
        bm.unpinPage(rootPid);
    }

    private byte[] toSize(byte[] arr, int size) {
        if (arr.length == size) {
            return arr;
        }

        int contentSize = arr.length < size ? arr.length : size;
        byte[] resized = new byte[size];

        for (int i = 0; i < contentSize; ++i) {
            resized[i] = arr[i];
        }

        return resized;
    }

    private void insertIntoLeafPage(byte[] key, Rid rid, int pageId, int[] parentPageIds){
        TabularPage leafPage = (TabularPage)bm.getPage(pageId, fileTitle);
        //we KNOW we are in a leaf page in this function
        if(leafPage.isFull()){
            //If page is full call handlesplit
            handleSplit(key, rid, pageId, parentPageIds, true);
        }
        //Now we know we can fit the key in, so first create the row we are going to insert eventually
        ByteBuffer b = ByteBuffer.allocate(leafNodeRowLength);
        b.put(key);
        b.putInt(rid.getPageId());
        b.putInt(rid.getSlotId());
        byte[] byteToInsert = b.array();
        Row rowToInsert = new Row(byteToInsert);
        //now we loop to find the proper place to insert
        int i = 1;
        while(i<leafPage.height()){
            Row currRow = leafPage.getRow(i);
            int comparison = Arrays.compare(key, 0, fixedKeyLength, currRow.data, 0, fixedKeyLength);
            if(comparison<=0){
                //MAYBE change to strictly less, could be slightly more efficient
                //key we are inserting is SMALLER than or equal to the current row key, and i is now saved as where we want to insert
                break;
            }
            i++;
        }
        //now i is the place where we want to insert
        Row nextRowToInsert = rowToInsert;
        while(i < leafPage.height()){
                //get what's currently at i and save it to be used in the next iteration of the loop
                Row currRow = leafPage.getRow(i);
                //modify the row at i with the new information we want there
                leafPage.modifyRow(nextRowToInsert, i);
                //save the current row so we can shift it over to the next position
                nextRowToInsert = currRow;
                i++;
        }
        //now we need to add on one new node at the end
        leafPage.insertRow(nextRowToInsert);
        //unpin the page
        bm.unpinPage(pageId);
    }

    private int getIntFromRow(Page nodePage, int rowId, int index){
        byte[] rowData = nodePage.getRow(rowId).data;
        return ByteBuffer.wrap(rowData).getInt(index*4);
    }
    //if isLeaf = True then rid is a "real" rid, if isLeaf=false then ONLY the pageid of rid matters
    private void handleSplit(byte[] key,Rid rid, int pageId, int[] parentPageIds, boolean isLeaf){
        //will have 2 pages pinned throughout this function
        TabularPage pageToSplit = (TabularPage)bm.getPage(pageId, fileTitle);
        int middleKeyId = pageToSplit.height()/2; //this is the number of rows currently in the page divided by 2
        //save the middleKey itself to use later
        byte[] middleKey = Arrays.copyOfRange(pageToSplit.getRow(middleKeyId).data, 0, fixedKeyLength);
        int currOldPageRowId = middleKeyId;
        //creation of new page
        int rowLength = -1;
        if(isLeaf){
            rowLength = leafNodeRowLength;
        }
        else{
            rowLength = nonLeafNodeRowLength;
        }
        TabularPage newPage = (TabularPage)bm.createPage(fileTitle, rowLength);
        int newPageId = newPage.getId();
        //a little optimization which ISN'T implemented could be check if the key to be inserted is in the second half of the page
        //i.e. it is in the new page, if that is the case then we could add it in appropriately in the below loop.
        int comparison = Arrays.compare(key, 0, fixedKeyLength, pageToSplit.getRow(middleKeyId).data, 0, fixedKeyLength);
        //may want to change to >, but i think this is correct
        /*if(comparison>=0){
            //now we continue on with optimized portion, a lot of copy and pasting
        }*/
    
        //we KNOW that comparison <0 --> key < middleKey --> where to insert key must be in
        //here we do things as normal
        //first we fill the newPage appropriately
        while(currOldPageRowId < pageToSplit.height()){
            newPage.insertRow(pageToSplit.getRow(currOldPageRowId));
            currOldPageRowId++;
        }
        //now the new page is filled properly, and we insert the key into the appropriate spot in pageToSplit aka oldPage
        //we also need to "delete" the moved entries from pageToSplit which is the same as setting newRowId appropriately
        //don't need +1 because middleKey in right page
        pageToSplit.set_nextRowId(middleKeyId);
        if(isLeaf){
            insertIntoLeafPage(key, rid, newPage.getId(), parentPageIds);
            //now setting the previous and next pointers
            //first we get the original ones out
            int ogPrev = getIntFromRow(pageToSplit, 0, 0);
            int ogNext = getIntFromRow(pageToSplit, 0, 1);
            //first put the older previous and newPageId as next in the pageToSplit page
            ByteBuffer pageToSplitBuff = ByteBuffer.allocate(pageToSplit.get_rowLength());
            pageToSplitBuff.putInt(ogPrev);
            pageToSplitBuff.putInt(newPageId);
            pageToSplit.modifyRow(new Row(pageToSplitBuff.array()), 0);
            
            //now put the pageToSplit as previous and the old next as the next pointer
            ByteBuffer newPageBuff = ByteBuffer.allocate(newPage.get_rowLength());
            newPageBuff.putInt(pageToSplit.getId());
            newPageBuff.putInt(ogNext);
            newPage.modifyRow(new Row(newPageBuff.array()), 0);

            //now previous and next should be properly created/modified.
            //if we have an internal/nonleaf node we do NOT need to worry about this process.
        }
        else{
            //insertIntoNonLeafPage(key, rid.getPageId(), newPage.getId(), parentPageIds)
        }
        bm.unpinPage(pageToSplit.getId(), fileTitle);
        bm.unpinPage(newPage.getId(), fileTitle);
    
        //now the pages are correctly created, we just need to "push up" the right value to insertIntoNonLeafPage
        //We NEED TO CHECK IF THIS IS THE ROOT, IF IT IS THEN WE MUST CREATE A NEW NODE TO PUSH UP INTO, and can just manually do it here
        if(pageId ==rootPid){
            //this means we are splitting the root, so need to make a new root that looks at this properly
            TabularPage newRoot = (TabularPage)bm.createPage(fileTitle, nonLeafNodeRowLength);
            //insert the first row which is just a page id, no key so...
            byte[] fakeKey = new byte[15];
            ByteBuffer newRootBuff = ByteBuffer.allocate(nonLeafNodeRowLength);
            newRootBuff.put(fakeKey);
            newRootBuff.putInt(pageId);
            byte[] byteToInsert = newRootBuff.array();
            newRoot.insertRow(new Row(byteToInsert));
            //also add the second row which is the middleKeyValue alongside the newPageId
            ByteBuffer newRootBuff2 = ByteBuffer.allocate(nonLeafNodeRowLength);
            newRootBuff2.put(middleKey);
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

}
