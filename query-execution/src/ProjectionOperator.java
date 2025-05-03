import java.nio.ByteBuffer;

public class ProjectionOperator implements Operator {

    private static boolean deleteOnClose = false;

    private Operator child;
    private int[][] srcRanges;
    private BufferManager bm;
    private boolean onTheFly;
    private boolean materialized;
    private Operator tempTableScan;
    private Relation projected;
    private int rowLength;

    private static int getRowLength(int[][] ranges) {
        int length = 0;
        for (int[] range : ranges) {
            length += range[1] - range[0];
        }
        return length;
    }

    public ProjectionOperator(Operator child, int[]... srcRanges) {
        this.child = child;
        this.srcRanges = srcRanges;
        bm = null;
        onTheFly = true;
        materialized = false;
        tempTableScan = null;
        projected = null;
        rowLength = getRowLength(srcRanges);
    }

    public ProjectionOperator(Operator child, BufferManager bm, int[]... srcRanges) {
        this.child = child;
        this.srcRanges = srcRanges;
        this.bm = bm; // important difference: when bm is passed, onTheFly is false so we will materialized upon next() call
        onTheFly = false;
        materialized = false;
        tempTableScan = null;
        projected = null;
        rowLength = getRowLength(srcRanges);
    }

    @Override
    public void open() {
        child.open();
        if (materialized) {
            tempTableScan.open();
        }
    }

    private Row nextFromChild() {
        Row originalRow = child.next();
        if (originalRow == null) {
            return null;
        }
        ByteBuffer newData = ByteBuffer.allocate(rowLength);
        for (int[] range : srcRanges) {
            newData.put(originalRow.viewRange(range));
        }
        return new Row(newData);
    }

    @Override
    public Row next() {
        if (onTheFly) {
            return nextFromChild();
        }
        if (materialized) {
            return tempTableScan.next();
        }
        int[][] attrRanges = new int[srcRanges.length][2];
        for (int i = 0; i < srcRanges.length; ++i) {
            attrRanges[i][0] = (i > 0) ? attrRanges[i - 1][1] : 0; // if this is the first range the start index is 0, otherwise it is the end index of the previous range
            attrRanges[i][1] = attrRanges[i][0] + srcRanges[i][1] - srcRanges[i][0]; // start of current range plus length of src range
        }
        projected = new Relation("Projection", attrRanges, bm, true);
        Row nextRow = nextFromChild();
        while (nextRow != null) {
            projected.insertRow(nextRow);
            nextRow = nextFromChild();
        }
        materialized = true;
        tempTableScan = new ScanOperator(projected, true); // if materialized, then one page is pinned
        tempTableScan.open();
        return tempTableScan.next();
    }

    @Override
    public void close() {
        child.close();
        if (materialized) {
            tempTableScan.close();
            if (deleteOnClose) {
                // need to delete temporary table? Right now, we don't delete, materialized stays true and tempTableScan can be reused
                projected.delete();
                materialized = false;
            }
        }
    }

}
