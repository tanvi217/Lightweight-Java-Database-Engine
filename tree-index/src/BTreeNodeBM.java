import java.util.ArrayList;
import java.util.List;

abstract class BTreeNodeBM{
    protected List<byte[]> keys;
    protected boolean isLeaf;
    protected BTreeNodeBM parent;
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

    public List<byte[]> getKeys() {
        return keys;
    }

    public BTreeNodeBM getParent() {
        return parent;
    }

    public void setParent(BTreeNodeBM parent) {
        this.parent = parent;
    }

    public List<BTreeNodeBM> getChildren() {
        return null;
    }

    public abstract void insert(byte[] key, Rid rid, BTreeIndexBM tree);
}
