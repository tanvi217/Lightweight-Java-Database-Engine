import java.nio.ByteBuffer;

/* TODO update comment
 * writes directly to the buffer stored in LRUBufferManager. This isn't
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

public class TabularPage implements Page {

    private static final int metadataBytes = 8;
    private int pageId;
    private ByteBuffer buffer;
    private int rowLengthLocation;
    private int nextRowIdLocation;
    private int rowLength;
    private int nextRowId; // equal to the number of full rows in this page
    private int maxRows;
    private Row[] rows;

    public TabularPage(int pageId, int bytesInRow, ByteBuffer pageData) {
        this.pageId = pageId;
        buffer = pageData.slice();
        int availableSpace = buffer.capacity() - metadataBytes; // capacity is equal to the size of the ByteBuffer slice
        rowLengthLocation = availableSpace; // the index directly after space which may be used for rows
        nextRowIdLocation = rowLengthLocation + 4; // the next four bytes (one int) over
        if (bytesInRow > 0) { // any positive rowLength is valid, and assume that this is a new page with no rows yet
            rowLength = bytesInRow;
            buffer.putInt(rowLengthLocation, rowLength);
            nextRowId = 0;
            buffer.putInt(nextRowIdLocation, nextRowId);
        } else { // otherwise read values from the byte buffer instead
            rowLength = buffer.getInt(rowLengthLocation);
            nextRowId = buffer.getInt(nextRowIdLocation);
        }
        maxRows = availableSpace / rowLength;
        rows = new Row[maxRows];
    }

    public TabularPage(int pageId, int pageStart, int pageLength, ByteBuffer fullData, int bytesInRow) {
        this(pageId, bytesInRow, fullData.clear().position(pageStart).limit(pageStart + pageLength));
    }

    private Row selectRow(int rowId) {
        int start = rowId * rowLength;
        buffer.position(start).limit(start + rowLength);
        Row selected = new Row(buffer, false); // copy flag set to false so that Row has same backing array as buffer
        buffer.clear();
        return selected;
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
    public void overwriteRow(Row row, int rowId) {
        if (rowId < 0 || rowId >= nextRowId){
            throw new IllegalArgumentException("Row index " + rowId + " out of bounds.");
        }
        int start = rowId * rowLength;
        buffer.position(start);
        if (row.length() >= rowLength) {
            buffer.put(row.viewRange(0, rowLength)); // insert full or truncated row
        } else { // passed row is too short, fill rest with zeros
            buffer.put(row.viewRange()); // insert full range of row (no arguments defaults to full range)
            byte[] trailingZeros = new byte[rowLength - row.length()];
            buffer.put(trailingZeros);
        }
        if (buffer.position() != start + rowLength) {
            throw new IllegalStateException("Unexpected error while overwriting row.");
        }
        buffer.clear();
        rows[rowId] = selectRow(rowId);
    }

    @Override
    public void insertRow(Row row, int rowId) {
        if (isFull()) {
            throw new IllegalArgumentException("No room to insert row.");
        }
        if (rowId < 0 || rowId > nextRowId){ // rowId can equal nextRowId during insertion
            throw new IllegalArgumentException("Row index out of bounds.");
        }
        int dstRowId = nextRowId;
        ++nextRowId; // increment since we are inserting. This needs to be done here so that dstRowId is a valid argument to overwriteRow
        buffer.putInt(nextRowIdLocation, nextRowId); // write new nextRowId to buffer, doesn't change buffer.position()
        while (dstRowId > rowId) {
            overwriteRow(selectRow(dstRowId - 1), dstRowId);
            --dstRowId;
        }
        overwriteRow(row, dstRowId); // dstRowId == rowId
    }

    @Override
    public int insertRow(Row row) {
        if (isFull()) {
            return -1; // insertion would exceed max number of rows
        }
        int rowId = nextRowId;
        insertRow(row, rowId); // increments nextRowId
        return rowId;
    }

    @Override
    public int height() {
        return nextRowId;
    }

    public int get_rowLength() {
        return rowLength;
    }

    @Override
    public void setHeight(int height) {
        nextRowId = height;
        buffer.putInt(nextRowIdLocation, nextRowId); // write new nextRowId to buffer, buffer.position() is unchanged
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
        int pageBytes = rowLengthLocation + metadataBytes;
        int firstInt = buffer.getInt(0);  // buffer.position() is unchanged
        String info = String.format("PAGE  id: %02d  rows: %03d  start-index: %04d  full-length: %d bytes  first-int: %d", pageId, nextRowId, buffer.arrayOffset(), pageBytes, firstInt);
        if (nextRowId != buffer.getInt(nextRowIdLocation)) {
            return "INCONSISTENT " + info;
        }
        return info;
    }

}
