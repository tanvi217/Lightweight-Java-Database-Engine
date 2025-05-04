public class ScanOperator implements Operator {

    private Relation relation;
    private boolean keepPinned;
    private Page pinned;
    private Rid nextRid;

    public ScanOperator(Relation relation, boolean keepPinned) {
        this.relation = relation;
        this.keepPinned = keepPinned;
        pinned = null;
        nextRid = null;
    }

    public ScanOperator(BufferManager bm, String tableTitle, int rowLength) {
        this(new Relation(tableTitle, rowLength, bm), false);
    }

    @Override
    public void open() {
        nextRid = new Rid(0, 0);
        if (keepPinned) {
            pinned = relation.getPage(0);
        }
    }

    @Override
    public Row next() {
        if (nextRid == null) {
            return null;
        }
        int pid = nextRid.getPageId();
        if (pid >= relation.getPageCount()) {
            throw new IllegalStateException("Unexpected error, next pid was invalid.");
        }
        int sid = nextRid.getSlotId();
        Page currentPage = keepPinned ? pinned : relation.getPage(pid); // note that pinned will have been set such that these are the same page
        int height = currentPage.height();
        if (sid >= height) {
            throw new IllegalStateException("Unexpected error, next sid was invalid.");
        }
        Row next = currentPage.getRow(sid).copy();
        if (!keepPinned) {
            relation.unpinPage(pid);
        }
        ++sid;
        if (sid == height) {
            ++pid;
            if (pid == relation.getPageCount()) {
                nextRid = null;
                return next;
            }
            if (keepPinned) {
                relation.unpinPage(pinned.getId());
                pinned = relation.getPage(pid);
            }
            sid = 0;
        }
        nextRid = new Rid(pid, sid);
        return next;
    }

    public Rid getNextRid() {
        return nextRid;
    }

    @Override
    public void close() {
        nextRid = null;
        if (keepPinned && pinned != null) {
            relation.unpinPage(pinned.getId());
            pinned = null;
        }
    }

}
