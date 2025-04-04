import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;

//important, each node is an ENTIRE PAGE. The rows size will be chosen accordingly
public class BTreeIndexBM{
    //we are going to store (mostly) everything as byte arrays and pages to make things easier
    //instead we can stored maxKeysBytes in the first row, since we won't have a key there anyway
    private TabularPage root; //root of the tree
    private boolean rootIsLeaf; //true if the root node is a leaf node, false otherwise
    private int maxKeys; //max nos. of keys in node
    private BufferManager bm;
    private int rootPid = 0;
    private final int fixedKeyLength = 15;
    private int leafNodeRowLength;

    public BTreeIndexBM(BufferManager bm) {
        //each entry (root starts as a leaf node) needs a k, i.e. the actual value the index is being sorted by
        //a rid which is a page id and a slot id (an int, i.e. 4 bytes allocated for every page id and slot id should be sufficient)
        //only ONCE at the beginning/end of the page we need a previous pointer and a next pointer, both page ids
        leafNodeRowLength=fixedKeyLength+8+2*4;
        this.root = (TabularPage)bm.createPage("BTree", (leafNodeRowLength));
        this.rootPid = root.getId();
        rootIsLeaf = true;
        this.bm = bm;
        //will initialize the previous and next pointers have 1s in the remaining fixedkeyLength-8 bytes
        byte[] initialNextPrev = new byte[fixedKeyLength];
        for(int i = 8; i < initialNextPrev.length; i++){
            initialNextPrev[i]=1;
        }
        root.insertRow(new Row(initialNextPrev));
    }
    //might want to add somewhere a truncating function, for movieTitles for example, because in page we truncate whole rows where
    //here a row can consist of multiple movietitles. Here is the function from another place...
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

    //new idea, ditch the classes they are just making it harder for me to think through, instead just have this file
    //we want another private insert function which we will call for everything past the root, we can think of as recursing down
    //but with a slightly different function
    public void insert(byte[] key, Rid rid){
        //first we create the Row we are actually going to insert into the page
        ByteBuffer b = ByteBuffer.allocate(fixedKeyLength);
        b.put(key);
        b.putInt(rid.getPageId());
        b.putInt(rid.getSlotId());
        byte[] byteToInsert = b.array();
        Row rowToInsert = new Row(byteToInsert);
        key = toSize(key, fixedKeyLength);
        //something to note, if there is not room to add we have to split the node. In this case we need to create a new node and 
        //add over the new data. Furthermore we need to implement deletions into page and to do that we can just set the nextNodeId
        //pointer to be way earlier and then for our purposes the data is 'deleted'
        if(rootIsLeaf){
            //in this case the root is just a leaf node, if there is room add to root
            if (!root.isFull()){
                //leaf nodes will always have their first entry instead be two pageIDs referring to previous and next respectively
                int i =1;
                while(i<root.get_nextRowId()){
                    Row currRow = root.getRow(i);
                    int comparison = Arrays.compare(currRow.data, 0, fixedKeyLength, key, 0, fixedKeyLength);
                    if(comparison==1){
                        //currRow is bigger so stop here, i is where we want to insert
                        break;
                    }
                    i++;
                }
                //now i is the place where we want to insert
                Row nextRowToInsert = rowToInsert;
                while(i < root.get_nextRowId()){
                    //get what's currently at i and save it to be used in the next iteration of the loop
                    Row currRow = root.getRow(i);
                    root.modifyRow(nextRowToInsert, i);
                    nextRowToInsert = currRow;
                    i++;
                }
                //now we need to add on one new node at the end
                root.insertRow(nextRowToInsert);
            }
        }
    }
    //temp is just so the above is a different function because I want to think througb logic from scratch
    public void insert(byte[] key, Rid rid, int temp) {    //to insert a new key in tree
        Row rootRow = rootPage.getRow(rootSid);
        //this for loop is looping through all the things in root, i.e. the keys and the values
        for(int i =0; i<maxKeys; i++){
            int start = i * 15;
            byte[] chunk = new byte[15];
            System.arraycopy(rootRow.data, start, chunk, 0, 15);
            //checking to see if chunk is 0's, because if it is then there is no key there and we can stop
            byte[] zeroArray = new byte[chunk.length]; // By default, this will be all zeros
            if (Arrays.equals(chunk, zeroArray)){
                break;
            }
            //otherwise we want to compare it, the below means if key <= chunk, LOOK INTO if it should be < or <=
            if(Arrays.compare(key, chunk) <= 0){
                //this means we are in the correct child
                int childPos = 15*maxKeys+2*i;
                //right now this is one byte storing this info but that might be too small
                int childPageId = rootRow.data[childPos];
                int childSlotId = rootRow.data[childPos+1];
                //now call new helper function insert2()
            }
            //Need to add a converter of some kind depedning on the type of the key (string, int etc.)
            //K tempKey = new Comparable(rootRow.data, start, 15, StandardCharsets.UTF_8).trim(); // Remove padding
            //tempKeys.add(tempKey);
        }
        //from here we can parse and create the object we have been using
        //I think it is easiest if we store these rows as key, key, key, ..., key, pageID, pageID, ..., pageID

        root.insert(key, rid, this);    //insertion starts at the root
    }

    public Iterator<Rid> search(K key) {
        BTreeNode<K> node = root;   //start from root
        while (!node.isLeaf()) {    //traverse down the tree to find correct leaf node
            BTreeInternalNode<K> internalNode = (BTreeInternalNode<K>) node;
            int pos = 0;
            //changed to pos +1 because if not then we could break things because we are not choosing a valid index
            while (pos + 1 < internalNode.getKeys().size() && key.compareTo(internalNode.getKeys().get(pos)) > 0) {
                pos++;
            }
            node = internalNode.getChildren().get(pos); //move to correct child node
        }

        //perform search after finding the leaf node
        BTreeLeafNode<K> leafNode = (BTreeLeafNode<K>) node;
        return leafNode.search(key);
    }
    //this is for moving the split upwards in the tree(so that splitting continues until root is updated)
    public void handleSplit(BTreeNode<K> oldNode, K splitKey, BTreeNode<K> newNode) {
        BTreeInternalNode<K> parent = (BTreeInternalNode<K>) oldNode.getParent();
        
        //create a new node if parent is null
        if (parent == null) {
            parent = new BTreeInternalNode<>();
            root = parent;
            oldNode.setParent(parent);
        }
        parent.insertInternal(splitKey, oldNode, newNode);  //insert split key into the parent node
    }

    public List<K> getRootKeys() {
        return root.getKeys();  //return the keys of the root node
    }

    public void setRoot(BTreeInternalNode<K> root) {
        this.root = root;   //to set the root
    }
}
