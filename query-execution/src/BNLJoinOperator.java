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
    private BufferManager bm;
    private int[] blockPids;
    private int nextBlockPid;
    private boolean blockPinned;
    private Relation outRelation;
    private Map<String, List<Row>> sortedRows;
    private Iterator<Row> joinedRows;

    public BNLJoinOperator(Operator outChild, Operator inChild, int[] outAttr, int[] inAttr, BufferManager bm, int blockSize) {
        if (outAttr[1] - outAttr[0] != inAttr[1] - inAttr[0]) {
            throw new IllegalArgumentException("Outer and inner attributes are different sizes.");
        }
        this.outChild = outChild;
        this.inChild = inChild;
        this.outAttr = outAttr;
        this.inAttr = inAttr;
        this.bm = bm;
        blockPids = new int[blockSize];
        nextBlockPid = -1; // if nextBlockPid is -1 then there are no more rows to read and this.next() == null
        blockPinned = false;
        outRelation = null;
        sortedRows = null;
        joinedRows = null;
    }

    private Row joinRows(Row outer, Row inner) { // the attribute being joined on is put at the front of the row, then the remaining attributes of the outer row, then the remaining attributes of the inner row
        int attrLength = inAttr[1] - inAttr[0];
        return new Row(ByteBuffer.allocate(outer.length() + inner.length() - attrLength)
            .put(inner.viewRange(inAttr))
            .put(outer.viewRange(0, outAttr[0]))
            .put(outer.viewRange(outAttr[1]))
            .put(inner.viewRange(0, inAttr[0]))
            .put(inner.viewRange(inAttr[1])).clear());
    }

    private void sortIntoHashMap(Row outer) {
        String attr = outer.getString(outAttr);
        if (!sortedRows.containsKey(attr)) {
            sortedRows.put(attr, new ArrayList<Row>());
        }
        sortedRows.get(attr).add(outer); // note that sorted rows could contain uncopied rows, which are connected directly to the byte array in buffer manager. This is ok since the rows won't move around while the block of clean pages is loaded
    }

    private boolean pinNewBlock() { // returns false if there are no more rows in this operator
        if (nextBlockPid == -1) {
            return false;
        }
        Row currRow = outChild.next();
        if (currRow == null) {
            return false;
        }
        if (outRelation == null) {
            outRelation = new Relation("JoinOuterTable", currRow.length(), bm, true);
        }
        sortedRows = new HashMap<>();
        int currRowPid = nextBlockPid; // the pid of the page currRow is from
        for (int i = 0; i < blockPids.length; ++i) {
            blockPids[i] = currRowPid;
            while (currRowPid == blockPids[i]) {
                sortIntoHashMap(currRow);
                currRowPid = outRelation.insertRow(currRow, true); // this is now pid of upcoming row, the one initialized in the next line
                if (currRowPid != blockPids[i] && i + 1 == blockPids.length) { // if the next row will be on a new page and we don't have room in this block
                    break; // skip the next section where we get outChild.next() in this special case
                }
                currRow = outChild.next();
                if (currRow == null) {
                    if (i + 1 < blockPids.length) {
                        blockPids[i + 1] = -1; // use negative flag so we don't read more pages when unpinning
                    }
                    outRelation.getPage(blockPids[i]); // pin, finished writing to this page
                    blockPinned = true;
                    return true;
                }
            }
            outRelation.getPage(blockPids[i]); // pin, finished writing to this page
        }
        blockPinned = true;
        nextBlockPid = currRowPid;
        return true;
    }

    private void unpinBlock() { // make sure that all blockPids before a negative entry are actually pinned
        sortedRows = null;
        joinedRows = null;
        if (!blockPinned) { // this is the only purpose of the blockPinned flag, making sure we don't unpin pages twice
            return;
        }
        for (int pid : blockPids) {
            if (pid < 0) {
                break;
            }
            // System.out.println(outRelation.tableTitle + " page " + pid + " unpinned");
            outRelation.unpinPage(pid);
        }
        blockPinned = false;
    }

    @Override
    public void open() {
        outChild.open();
        inChild.open();
        nextBlockPid = 0;
        boolean outEOF = !pinNewBlock(); // pinNewBlock() returns false if we have reached the end of the outChild operator
        if (outEOF) {
            nextBlockPid = -1;
        }
        // in the case where pinNewBlock() returns true, we have successfully set blockPids, nextBlockPid, outRelation, sortedRows, and joinedRows
    }

    @Override
    public Row next() {
        if (nextBlockPid == -1) {
            return null;
        }
        while (true) {
            if (joinedRows != null && joinedRows.hasNext()) {
                return joinedRows.next();
            }
            Row innerRow = inChild.next();
            if (innerRow == null) { // inner EOF
                unpinBlock();
                boolean outEOF = !pinNewBlock();
                if (outEOF) { // outer and inner EOF
                    nextBlockPid = -1;
                    return null;
                }
                inChild.close();
                inChild.open();
                innerRow = inChild.next();
                if (innerRow == null) { // inChild has no rows, so joined table will also have no rows
                    nextBlockPid = -1;
                    return null;
                }
            } // now we have a valid inner row and a pinned block of outer rows
            String attr = innerRow.getString(inAttr);
            List<Row> matches = sortedRows.get(attr);
            if (matches == null) { // no matches
                joinedRows = null;
            } else {
                List<Row> joinedList = new ArrayList<>(matches.size());
                for (Row outerMatch : matches) {
                    joinedList.add(joinRows(outerMatch, innerRow));
                }
                joinedRows = joinedList.iterator();
            }
        }
    }

    @Override
    public void close() {
        unpinBlock();
        outChild.close();
        inChild.close();
        nextBlockPid = -1;
    }

}
