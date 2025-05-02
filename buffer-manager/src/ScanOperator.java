public class ScanOperator implements Operator {
    private BufferManager bm;
    private String table;
    private int rowLen;
    private int pageId;
    private Page page;
    private int rid;

    public ScanOperator(BufferManager bm, String table, int rowLen) {
        this.bm = bm;
        this.table = table;
        this.rowLen = rowLen;
    }

    @Override
    public void open() {
        pageId = 0;
        rid = 0;
        try {
            page = bm.getPage(pageId, table);

            System.out.println("Opened table: " + table + ", pageId: " + pageId + ", rowLen: " + rowLen);
        } catch (IllegalArgumentException e) {
            page = null;
        }
    }

    @Override
    public Row next() {
        while (page != null) {
            if (rid < page.height()) {
                return page.getRow(rid++);
            } else {
                bm.unpinPage(page.getId(), table);
                pageId++;
                rid = 0;

                try {
                    page = bm.getPage(pageId, table);
                } catch (IllegalArgumentException e) {
                    page = null;
                }
            }
        }
        return null;
    }

    @Override
    public void close() {
        if (page != null) {
            bm.unpinPage(page.getId(), table);
            System.out.println("Closed table: " + table + ", pageId: " + pageId);
            page = null;
        }
    }
}