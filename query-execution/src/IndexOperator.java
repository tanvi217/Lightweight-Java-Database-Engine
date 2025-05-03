import java.util.Iterator;

public class IndexOperator implements Operator {

    private static int groupSize = 100; // when we search the BTree, retrieve results in groups of this amount, so we don't have to traverse the tree so often
    private Relation relation;
    private int[] attr;
    private String startKey;
    private BufferManager bm;
    private BTree<String> bTree;
    private Iterator<Rid> nextRids;

    // will retrieve rows from the given relation in sorted order of passed string attribute, starting at startKey
    public IndexOperator(Relation relation, int[] attr, String startKey, BufferManager bm) {
        this.relation = relation;
        this.attr = attr;
        this.startKey = startKey;
        this.bm = bm;
        bTree = null;
        nextRids = null;
    }

    @Override
    public void open() {
        bTree = new ByteKeyBTree<>(bm, attr);
        ScanOperator scan = new ScanOperator(relation, false);
        scan.open();
        Rid nextRid = scan.getNextRid();
        int nextPid = nextRid.getPageId();
        int nextSid = nextRid.getSlotId();
        Row nextRow = scan.next(); // nextPid and nextRid should lead to this row
        while (nextRow != null) {
            String attrString = nextRow.getString(attr);
            bTree.insert(attrString, new Rid(nextPid, nextSid));
            nextRid = scan.getNextRid();
            nextPid = nextRid.getPageId();
            nextSid = nextRid.getSlotId();
            nextRow = scan.next();
        }
        scan.close();
        nextRids = bTree.groupSearch(startKey, groupSize);
    }

    public void reopen(String newStartKey) {
        nextRids = bTree.groupSearch(newStartKey, groupSize);
    }

    private Iterator<Rid> getNextRids(Rid lastRid) {
        int pid = lastRid.getPageId();
        int sid = lastRid.getSlotId();
        String nextKey = relation.getPage(pid).getRow(sid).getString(attr);
        relation.unpinPage(pid);
        return bTree.groupSearch(nextKey, groupSize);
    }

    @Override
    public Row next() {
        if (nextRids == null || !nextRids.hasNext()) {
            return null;
        }
        Rid next = nextRids.next();
        if (!nextRids.hasNext()) { // Rid 'next' was last element of nextRids
            nextRids = getNextRids(next);
            if (nextRids.hasNext()) { // the new group of Rids
                next = nextRids.next(); // first element of nextRids should be same as last element of the previous iterator
            }
        }
        int pid = next.getPageId();
        int sid = next.getSlotId();
        Row retrieved = relation.getPage(pid).getRow(sid); // Note that it is not copied
        relation.unpinPage(pid);
        return retrieved;
    }

    @Override
    public void close() {
        nextRids = null;
        // delete bTree somehow? not good if using reopen()
    }

}
