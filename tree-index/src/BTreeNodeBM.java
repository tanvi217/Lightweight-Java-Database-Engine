import java.util.ArrayList;
import java.util.List;

abstract class BTreeNodeBM<K extends Comparable<K>> {
    protected List<K> keys;
    protected boolean isLeaf;
    protected BTreeNode<K> parent;
    protected BufferManager bm;

    public BTreeNodeBM(boolean isLeaf, BufferManager bm) {
        this.isLeaf = isLeaf;
        this.bm = bm;
        this.keys = new ArrayList<>();
        this.parent = null;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public List<K> getKeys() {
        return keys;
    }

    public BTreeNode<K> getParent() {
        return parent;
    }

    public void setParent(BTreeNode<K> parent) {
        this.parent = parent;
    }

    public List<BTreeNode<K>> getChildren() {
        return null;
    }

    public abstract void insert(K key, Rid rid, BTreeIndex<K> tree);
}
