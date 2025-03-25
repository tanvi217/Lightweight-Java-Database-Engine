import java.nio.ByteBuffer;

public class TabularPage implements Page {

    private static final int METADATA_INTS = 2;
    private int pageId;
    private int rowLength;
    private ByteBuffer buffer;
    private int maxRows;
    private int pageStart; // index of first byte of this page in buffer
    private int nextRowIdLocation;
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
     * For retrieving pages
     * @param pageId
     * @param frameIndex
     * @param pageStart
     * @param pageLength
     * @param buffer
     */
    public TabularPage(int pageId, int pageStart, int pageLength, ByteBuffer buffer) {
        this.pageId = pageId;
        this.buffer = buffer;
        rowLengthLocation = pageStart + pageLength - METADATA_INTS * 4;
        nextRowIdLocation = rowLengthLocation + 4; // one int over
        rowLength = buffer.getInt(rowLengthLocation);
        nextRowId = buffer.getInt(nextRowIdLocation);
        maxRows = (pageLength - METADATA_INTS * 4) / rowLength;
        rows = new Row[maxRows];
    }

    /**
     * for new pages
     * @param pageId
     * @param frameIndex
     * @param pageStart
     * @param pageLength
     * @param buffer
     * @param rowLength
     */
    public TabularPage(int pageId, int pageStart, int pageLength, ByteBuffer buffer, int rowLength) {
        this.pageId = pageId;
        this.buffer = buffer;
        rowLengthLocation = pageStart + pageLength - METADATA_INTS * 4;
        nextRowIdLocation = rowLengthLocation + 4; // one int over
        buffer.putInt(rowLengthLocation, rowLength);
        buffer.putInt(nextRowIdLocation, 0);
        this.rowLength = rowLength;
        nextRowId = 0;
        maxRows = (pageLength - METADATA_INTS * 4) / rowLength;
        rows = new Row[maxRows];
    }

    @Override
    public Row getRow(int rowId) {
        if (rowId < 0 || rowId >= nextRowId) {
            throw new IllegalArgumentException("Row index out of bounds.");
        }

        if (rows[rowId] != null) {
            return rows[rowId];
        }

        int rowStart = pageStart + rowId * rowLength;
        byte[] data = new byte[rowLength];
        buffer.position(rowStart);
        buffer.get(data); // retrieve data from buffer
        rows[rowId] = new Row(data);

        return rows[rowId];
    }

    @Override
    public int insertRow(Row row) {
        if (nextRowId >= maxRows) {
            return -1;
        }
        int rowId = nextRowId;
        int rowStart = pageStart + rowId * rowLength;
        buffer.position(rowStart);
        buffer.put(toSize(row.data, rowLength)); // write data to buffer
        rows[rowId] = row;
        ++nextRowId;
        buffer.putInt(nextRowIdLocation, nextRowId); // write new nextRowId to buffer
        return rowId;
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
        int firstInt = buffer.getInt(pageStart);
        String info = String.format("PAGE  id: %02d  rows: %03d  start-index: %04d  full-length: %d bytes  first-int: %d", pageId, nextRowId, pageStart, pageBytes, firstInt);
        if (nextRowId != buffer.getInt(nextRowIdLocation)) {
            return "INCONSISTENT " + info;
        }
        return info;
    }

    /**
     * If the passed byte array's length is less than the given size, a new array
     * padded with zeros is returned. If the array is too long, it is trimmed to
     * size.
     */
    private byte[] toSize(byte[] arr, int size) {
        if (arr.length == size) {
            return arr;
        }

        int contentSize = arr.length < size ? arr.length : size;
        byte[] resized = new byte[size];

        for (int i = 0; i < contentSize; ++i) {
            resized[i] = arr[i];
        }

        return resized;
    }
}
