import java.nio.ByteBuffer;

public class TabularPage implements Page {

    private static final int METADATA_INTS = 2;
    private int pageId;
    private int rowLength;
    private ByteBuffer buffer;
    private int maxRows;
    private int pageStart; // index of first byte of this page in buffer
    public int nextRowIdLocation;
    private int rowLengthLocation;
    private Row[] rows;
    private int nextRowId; // equal to the number of full rows in this page

    /*
     * IMDbPage writes directly to the buffer stored in LRUBufferManager. This isn't
     * inefficient, because we don't write to disk at all. The number of bytes
     * available to store rows in given by pageBytes - 1, where we subtract one to
     * make space for nextRowId to be stored in a single byte at the end of the
     * page. The rows array starts full of null values, and is populated as calls to
     * getRow are made. The nextRowId is retrieved from the buffer at the end of
     * this page.
     * 
     * @param pageId     The id of this page.
     * @param frameIndex The location of this page in the buffer, used to calculate
     *                   pageStart.
     * @param pageBytes  Number of bytes in one page, is constant among all pages of
     *                   the same buffer manager.
     * @param buffer     A reference to the buffer initialized in LRUBufferManager.
     */

    /**
     * for new pages
     * @param pageId
     * @param frameIndex
     * @param pageStart
     * @param pageLength
     * @param buffer
     * @param rowLength
     */
    public TabularPage(int pageId, int pageStart, int pageLength, ByteBuffer buffer, int bytesInRow) {
        this.pageId = pageId;
        this.buffer = buffer;
        this.pageStart = pageStart;
        rowLengthLocation = pageStart + pageLength - METADATA_INTS * 4;
        nextRowIdLocation = rowLengthLocation + 4; // one int over
        buffer.clear();
        if (bytesInRow == 0) {
            nextRowId = buffer.getInt(nextRowIdLocation);
            rowLength = buffer.getInt(rowLengthLocation);
        } else {
            buffer.putInt(rowLengthLocation, bytesInRow);
            buffer.putInt(nextRowIdLocation, 0);
            rowLength = bytesInRow;
            nextRowId = 0;
        }
        maxRows = (pageLength - METADATA_INTS * 4) / rowLength;
        rows = new Row[maxRows];
    }

    private Row selectRow(int rowId) {
        int startIndex = pageStart + rowId * rowLength;
        buffer.clear();
        buffer.position(startIndex);
        buffer.limit(startIndex + rowLength);
        return new Row(buffer);
    }

    @Override
    public Row getRow(int rowId) {
        if (rowId < 0 || rowId >= nextRowId) {
            throw new IllegalArgumentException("Row index " + rowId + " out of bounds.");
        }
        if (rows[rowId] == null) {
            rows[rowId] = selectRow(rowId);
        }
        return rows[rowId];
    }

    // modifies an existing row, based on its rowId and replaces it with the given Row
    @Override
    public void overwriteRow(Row row, int rowId){
        if (rowId < 0 || rowId >= nextRowId){
            throw new IllegalArgumentException("Row index " + rowId + " out of bounds.");
        }
        int rowStart = pageStart + rowId * rowLength;
        buffer.clear();
        buffer.position(rowStart);
        if (row.length() >= rowLength) {
            buffer.put(row.getRange(0, rowLength)); // insert full or truncated row
        } else { // passed row is too short, fill rest with zeros
            buffer.put(row.getRange()); // insert full range of row (no arguments defaults to full range)
            byte[] trailingZeros = new byte[rowLength - row.length()];
            buffer.put(trailingZeros);
        }
        rows[rowId] = selectRow(rowId);
    }

    @Override
    public void insertRow(Row row, int rowId) {
        if (nextRowId >= maxRows) {
            throw new IllegalArgumentException("No room to insert row.");
        }
        if (rowId < 0 || rowId > nextRowId){ // rowId can equal nextRowId during insertion
            throw new IllegalArgumentException("Row index out of bounds.");
        }
        int dstRowId = nextRowId;
        ++nextRowId; // increment since we are inserting. This needs to be done here so that dstRowId is a valid argument to overwriteRow
        buffer.clear();
        buffer.putInt(nextRowIdLocation, nextRowId); // write new nextRowId to buffer
        while (dstRowId > rowId) {
            overwriteRow(selectRow(dstRowId - 1), dstRowId);
            --dstRowId;
        }
        overwriteRow(row, dstRowId); // i == rowId
    }

    @Override
    public int insertRow(Row row) {
        if (nextRowId >= maxRows) {
            return -1; // insertion would exceed max number of rows
        }
        int rowId = nextRowId;
        insertRow(row, rowId); // increments nextRowId
        return rowId;
    }

    public int height(){
        return nextRowId;
    }

    public int get_rowLength(){
        return rowLength;
    }

    public void setHeight(int height){
        nextRowId = height;
        buffer.clear();
        buffer.putInt(nextRowIdLocation, nextRowId); // write new nextRowId to buffer
    }

    @Override
    public boolean isFull() {
        return nextRowId >= maxRows;
    }

    @Override
    public int getId() {
        return pageId;
    }

    @Override
    public String toString() {
        int pageBytes = rowLengthLocation + METADATA_INTS * 4 - pageStart;
        buffer.clear();
        int firstInt = buffer.getInt(pageStart);
        String info = String.format("PAGE  id: %02d  rows: %03d  start-index: %04d  full-length: %d bytes  first-int: %d", pageId, nextRowId, pageStart, pageBytes, firstInt);
        if (nextRowId != buffer.getInt(nextRowIdLocation)) {
            return "INCONSISTENT " + info;
        }
        return info;
    }

}
