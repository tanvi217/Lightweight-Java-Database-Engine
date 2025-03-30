import java.util.Iterator;
import java.util.List;

public class BTreeIndex<K extends Comparable<K>> {
    private BTreeNode<K> root; //root of the tree
    private int maxKeys; //max nos. of keys in node

    public BTreeIndex(int maxKeys) {
        this.root = new BTreeLeafNode<>();  //leaf node as the root at start
        this.maxKeys = maxKeys;    //setting max keys for nodes
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public void insert(K key, Rid rid) {    //to insert a new key in tree
        root.insert(key, rid, this);    //insertion starts at the root
    }

    public Iterator<Rid> search(K key) {
        BTreeNode<K> node = root;   //start from root
        while (!node.isLeaf()) {    //traverse down the tree to find correct leaf node
            BTreeInternalNode<K> internalNode = (BTreeInternalNode<K>) node;
            int pos = 0;
            while (pos < internalNode.getKeys().size() && key.compareTo(internalNode.getKeys().get(pos)) > 0) {
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
