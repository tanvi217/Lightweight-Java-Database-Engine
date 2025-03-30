import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class BTreeLeafNode<K extends Comparable<K>> extends BTreeNode<K> {
    private List<Rid> values;   //list of rids with keys
    private BTreeLeafNode<K> next;  //link to next leaf

    public BTreeLeafNode() {
        super(true);
        this.values = new ArrayList<>();
        this.next = null;
    }

    public void insert(K key, Rid rid, BTreeIndex<K> tree) {
        int pos = 0;
        //insert at correct pos
        while (pos < keys.size() && key.compareTo(keys.get(pos)) > 0) {
            pos++;
        }
        keys.add(pos, key); //insert key at correct pos
        values.add(pos, rid);   //insert val

        if (keys.size() > tree.getMaxKeys()) {
            split(tree);    //split the node if it is full
        }
    }

    private void split(BTreeIndex<K> tree) {
        int mid = keys.size() / 2;  //find middle key for split
        BTreeLeafNode<K> newNode = new BTreeLeafNode<>();
        newNode.keys.addAll(keys.subList(mid, keys.size()));    //move the right half of keys
        newNode.values.addAll(values.subList(mid, values.size()));  //move right half of vals

        keys.subList(mid, keys.size()).clear(); //to clear current node after split
        values.subList(mid, values.size()).clear();

        newNode.next = this.next;   //link to next leaf node
        this.next = newNode;    //link this node to the new leaf node

        if (this.getParent() == null) { //create a new node if no parent is found
            BTreeInternalNode<K> newRoot = new BTreeInternalNode<>();
            tree.setRoot(newRoot);  //set the new root
            newRoot.insertInternal(newNode.keys.get(0), this, newNode); //insert split key into root
            this.setParent(newRoot);    //set parent for both nodes
            newNode.setParent(newRoot);
        } else {
            newNode.setParent(this.getParent());
            tree.handleSplit(this, newNode.keys.get(0), newNode);   //move split to parent nodes
        }
    }

    public Iterator<Rid> search(K key) {
        List<Rid> results = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).equals(key)) {
                results.add(values.get(i));     //add Rid if key is found
            }
        }
        return results.iterator();  //return an iterator for the res
    }

    public Iterator<Rid> rangeSearch(K startKey, K endKey) {
        List<Rid> results = new ArrayList<>();
        BTreeLeafNode<K> node = this;

        while (node != null) {
            for (int i = 0; i < node.keys.size(); i++) {
                K key = node.keys.get(i);
                if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {
                    results.add(node.values.get(i));    //add Rids within range
                }
            }
            node = node.next;   //move to next leaf
        }
        return results.iterator();
    }
}
