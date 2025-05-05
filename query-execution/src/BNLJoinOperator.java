import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BNLJoinOperator implements Operator {

    private Operator outChild;
    private Operator inChild;
    private int[] outAttr;
    private int[] inAttr;
    private int[] blockPids;
    private boolean blockPinned;
    private Relation outRelation;
    private int nextBlockPid;
    private Map<String, List<Row>> sortedRows;
    private Iterator<Row> joinedRows;
    private int[][] ranges;
    private int joinedRowLength;

    private void calculateRanges(int outRowLength, int inRowLength) {
        int attrLength = outAttr[1] - outAttr[0]; // would be same using inAttr
        joinedRowLength = outRowLength + inRowLength - attrLength;
        int[] outBefore = new int[] {0, outAttr[0]};
        int[] outAfter = new int[] {outAttr[1], outRowLength};
        int[] inBefore = new int[] {0, inAttr[0]};
        int[] inAfter = new int[] {inAttr[1], inRowLength};
        ranges = new int[][] {outBefore, outAfter, inBefore, inAfter};
    }

    public BNLJoinOperator(Operator outChild, Operator inChild, int[] outAttr, int[] inAttr, int outRowLength, int inRowLength, BufferManager bm, int blockSize) {
        if (outAttr[1] - outAttr[0] != inAttr[1] - inAttr[0]) {
            throw new IllegalArgumentException("Outer and inner attributes are different sizes.");
        }
        this.outChild = outChild;
        this.inChild = inChild;
        this.outAttr = outAttr;
        this.inAttr = inAttr;
        blockPids = new int[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            blockPids[i] = -1;
        }
        blockPinned = false;
        outRelation = new Relation("JoinOuterLeftTable", outRowLength, bm, true);
        nextBlockPid = 0;
        sortedRows = null;
        joinedRows = null;
        calculateRanges(outRowLength, inRowLength);
    }

    private void insertIntoHashMap(Row row) {
        String attr = row.getString(outAttr);
        if (!sortedRows.containsKey(attr)) {
            sortedRows.put(attr, new ArrayList<Row>());
        }
        sortedRows.get(attr).add(row); // note that sorted rows contains uncopied rows, which are connected directly to the byte array in buffer manager. This is ok since the rows won't move around while the block of clean pages is loaded
    }

    private boolean loadNewBlock() { // returns false if unsuccessful
        if (nextBlockPid == -1) {
            return false;
        }
        Row currRow = outChild.next();
        if (currRow == null) {
            outChild.close(); // EOF
            return false;
        }
        sortedRows = new HashMap<>();
        insertIntoHashMap(currRow);
        int nextRowPid = outRelation.insertRow(currRow);
        if (nextBlockPid != nextRowPid) {
            throw new IllegalStateException("Unexpected error, insertRow prediction was wrong. (or page only had one row)");
        }
        for (int i = 0; i < blockPids.length; ++i) {
            blockPids[i] = nextRowPid;
            while (nextRowPid == blockPids[i]) {
                currRow = outChild.next();
                System.out.println(currRow);
                if (currRow == null) {
                    nextBlockPid = -1;
                    outChild.close(); // EOF
                    if (i + 1 < blockPids.length) {
                        blockPids[i + 1] = -1; // use negative flag so we don't read more pages later on
                    }
                    outRelation.getPage(blockPids[i]); // pin
                    outRelation.markClean(blockPids[i]); // finished writing to this page
                    blockPinned = true;
                    return true;
                }
                insertIntoHashMap(currRow);
                nextRowPid = outRelation.insertRow(currRow);
            }
            outRelation.getPage(blockPids[i]); // pin
            outRelation.markClean(blockPids[i]); // finished writing to this page
        }
        blockPinned = true;
        nextBlockPid = nextRowPid;
        return true;
    }

    private void unloadBlock() { // make sure that all blockPids before a negative entry are actually pinned
        sortedRows = null;
        if (!blockPinned) {
            return;
        }
        for (int i = 0; i < blockPids.length; ++i) {
            if (blockPids[i] < 0) {
                return;
            }
            outRelation.unpinPage(blockPids[i]);
            blockPids[i] = -1;
        }
        blockPinned = false;
    }

    @Override
    public void open() {
        outChild.open();
        inChild.open();
        boolean success = loadNewBlock();
        if (!success) {
            throw new IllegalStateException("Unexpected error, could not load first block.");
        }
    }

    @Override
    public Row next() {
        // WRONG, we are not going through the block if it is only partly full, NOT WHAT WE WANT
        if (nextBlockPid == -1) {
            return null;
        }
        if (joinedRows != null && joinedRows.hasNext()) {
            System.out.println("here");
            return joinedRows.next();
        }
        Row innerRow = inChild.next();
        System.out.println(innerRow);
        if (innerRow == null) { // inner EOF
            inChild.close();
            unloadBlock();
            boolean success = loadNewBlock();
            if (!success) { // outer and inner EOF
                return null;
            }
            inChild.open();
            innerRow = inChild.next();
            if (innerRow == null) {
                throw new IllegalStateException("Inner child has no rows.");
            }
        }
        String attr = innerRow.getString(inAttr);
        List<Row> matches = sortedRows.get(attr);
        if (matches == null) { // no matches
            joinedRows = null;
        } else {
            List<Row> joinedList = new ArrayList<>(matches.size());
            for (Row outMatch : matches) {
                ByteBuffer joinedData = ByteBuffer.allocate(joinedRowLength);
                joinedData.put(outMatch.viewRange(outAttr)); // could use innerRow instead
                joinedData.put(outMatch.viewRange(ranges[0]));
                joinedData.put(outMatch.viewRange(ranges[1]));
                joinedData.put(innerRow.viewRange(ranges[2]));
                joinedData.put(innerRow.viewRange(ranges[3]));
                joinedList.add(new Row(joinedData.clear()));
            }
            joinedRows = joinedList.iterator();
        }
        return next();
    }

    @Override
    public void close() {
        outChild.close();
        inChild.close();
        nextBlockPid = 0;
        sortedRows = null;
        joinedRows = null;
        unloadBlock();
        for (int i = 0; i < blockPids.length; ++i) {
            blockPids[i] = -1;
        }
    }

}
