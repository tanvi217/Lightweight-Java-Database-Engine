import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

class BTreeInternalNodeBM<K extends Comparable<K>> extends BTreeNodeBM<K> {
    private List<BTreeNode<K>> children;    //child nodes of this internal node

    public BTreeInternalNodeBM(BufferManager bm) {
        super(false, bm);   //internal nodes are not leaves
        this.children = new ArrayList<>();  //initialize the children list
    }
    //this is for inserting a key into internal node
    public void insert(K key, Rid rid, BTreeIndex<K> tree) {        //insert a key to this internal node
        //root can change as time goes on
        int pos = 0;    
        
        //find correct pos for insertion
        //want to allow it to go up to one more than current size of keys as we can extend keys if this new key is the largest.
        while (pos < keys.size() && key.compareTo(keys.get(pos)) > 0) {
            pos++;
        }

        //recursively insert into correct child
        if (pos < children.size()) {  
            BTreeNode<K> child = children.get(pos);
            child.insert(key, rid, tree);
        }
        //split node if internal node is full
        if (keys.size() > tree.getMaxKeys()) {
            split(tree);
        }
    }

    public void split(BTreeIndex<K> tree) { //split internl node when it exceeds max keys
        int mid = keys.size() / 2;  //find middle key to split
        BTreeInternalNode<K> newNode = new BTreeInternalNode<>();
        
        //move right half of the keys and children to new node
        newNode.keys.addAll(keys.subList(mid + 1, keys.size()));
        newNode.children.addAll(children.subList(mid + 1, children.size()));
        //update parent of the children nodes
        for (BTreeNode<K> child : newNode.children) {
            child.setParent(newNode);
        }
        //remove moved keys and children from the current node
        keys.subList(mid + 1, keys.size()).clear();
        children.subList(mid + 1, children.size()).clear();
        //move middle key up to the parent
        K splitKey = keys.remove(mid);
        newNode.setParent(this.getParent());
        //move the split to the parent to do it
        tree.handleSplit(this, splitKey, newNode);
    }
    //this is for  inserting a key and its 2 child nodes into an internal node(during split of an inetrnal node)
    public void insertInternal(K key, BTreeNode<K> leftChild, BTreeNode<K> rightChild) {
        int pos = 0;
        while (pos < keys.size() && key.compareTo(keys.get(pos)) > 0) {
            pos++;
        }
        if (keys.isEmpty() || pos >= keys.size()) {
            keys.add(key);
            children.add(rightChild);
        } else {
            keys.add(pos, key);
            children.add(pos + 1, rightChild);
        }
        rightChild.setParent(this);
    }

    public List<BTreeNode<K>> getChildren() {
        return children;
    }

    public byte[] serialize(){
        //todo
        return new byte[2];
    }
}
