import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BTreeIndexBM<K extends Comparable<K>> {
    private BTreeNode<K> root; //root of the tree
    private int maxKeys; //max nos. of keys in node
    private BufferManager bm;
    private int rootPid = 0;
    private int rootSid = 0;

    public BTreeIndexBM(int maxKeys, BufferManager bm) {
        this.root = new BTreeLeafNode<>();  //leaf node as the root at start
        this.maxKeys = maxKeys;    //setting max keys for nodes
        this.bm = bm;
        //for now let's say rowsize is maxKeys*15, 15 bytes for each key + maxKeys*1+1 for each pagereference.
        //E.g. if maxKeys = 5 we need to store 5, 15 length strings, and 6 pageIds, one referencing those below the first string, one for each in between, and for each above the 5th
        bm.createPage("BTree", 15*maxKeys+maxKeys+1);
        //pageID should be 0
        //might want to add somewhere a truncating function, for movieTitles for example, because in page we truncate whole rows where
        //here a row can consist of multiple movietitles. Here is the function from another place...
        /*
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
         */
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public void insert(K key, Rid rid) {    //to insert a new key in tree
        Row rootRow = bm.getPage(rootPid, "BTree").getRow(rootSid);
        List<K> tempKeys = new ArrayList<>();
        //this for loop is wrong right now
        for(int i =0; i<maxKeys; i++){
            int start = i * 15;
            byte[] chunk = new byte[15];
            System.arraycopy(rootRow.data, start, chunk, 0, 15);
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
