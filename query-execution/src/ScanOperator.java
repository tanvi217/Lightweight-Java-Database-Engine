public class ScanOperator implements Operator {

    private Relation relation;
    private Rid nextRid;

    public ScanOperator(Relation relation) {
        this.relation = relation;
        nextRid = null;
    }

    public ScanOperator(BufferManager bm, String tableTitle, int rowLength) {
        this(new Relation(tableTitle, rowLength, bm));
    }

    @Override
    public void open() {
        nextRid = new Rid(0, 0);
        System.out.println("Opened table: " + relation.tableTitle + ", pageId: 0, rowLen: " + relation.bytesInRow);
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
        Page currentPage = relation.getPage(pid);
        int height = currentPage.height();
        if (sid >= height) {
            throw new IllegalStateException("Unexpected error, next sid was invalid.");
        }
        Row next = currentPage.getRow(sid); // Note that the row is not copied and subject to change, is this OK? otherwise call .copy() on this variable and return the result
        relation.unpinPage(pid);
        ++sid;
        if (sid == height) {
            ++pid;
            if (pid == relation.getPageCount()) {
                nextRid = null;
                return next;
            }
            sid = 0;
        }
        nextRid = new Rid(pid, sid);
        return next;
    }

    @Override
    public void close() {
        System.out.println("Closed table: " + relation.tableTitle + ", pageId: " + nextRid.getPageId());
        nextRid = null;
    }

}
