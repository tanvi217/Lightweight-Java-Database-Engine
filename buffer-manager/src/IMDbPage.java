import java.nio.ByteBuffer;

public class IMDbPage implements Page {

    private static int rowBytes = 39;
    private int pageId;
    private ByteBuffer buffer;
    private int maxRows;
    private int pageStart; // index of first byte of this page in buffer
    private int lastByteIndex;
    private Row[] rows;
    private int nextRowId; // equal to the number of full rows in this page

    /**
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
    public IMDbPage(int pageId, int frameIndex, int pageBytes, ByteBuffer buffer, boolean isEmpty) {
        this.pageId = pageId;
        this.buffer = buffer;
        maxRows = (pageBytes - 1) / rowBytes;
        pageStart = frameIndex * pageBytes;
        lastByteIndex = pageStart + pageBytes - 1;
        rows = new Row[maxRows];
        if (isEmpty) {
            buffer.put(lastByteIndex, (byte) 0);
        }
        nextRowId = (int) buffer.get(lastByteIndex);
    }

    @Override
    public Row getRow(int rowId) {
        if (rowId < 0 || rowId >= nextRowId) {
            throw new IllegalArgumentException("Row index out of bounds.");
        }
        if (rows[rowId] != null) {
            return rows[rowId];
        }
        int rowStart = pageStart + rowId * rowBytes;
        byte[] movieId = new byte[9];
        byte[] title = new byte[30];
        buffer.position(rowStart);
        buffer.get(movieId); // retrieve data from buffer
        buffer.get(title);
        rows[rowId] = new OriginalRow(movieId, title);
        return rows[rowId];
    }

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

    @Override
    public int insertRow(Row row) {
        if (nextRowId >= maxRows) {
            return -1;
        }
        int rowId = nextRowId;
        int rowStart = pageStart + rowId * rowBytes;
        buffer.position(rowStart);
        buffer.put(toSize(row.movieId, 9)); // write data to buffer
        buffer.put(toSize(row.title, 30));
        rows[rowId] = row;
        ++nextRowId;
        buffer.put(lastByteIndex, (byte) nextRowId); // write new nextRowId to buffer
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
    public byte[] serialize() {
        throw new UnsupportedOperationException("Unimplemented method 'serialize'");
    }

    @Override
    public boolean deserialize(byte[] data) {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }

    @Override
    public String toString() {
        int pageBytes = lastByteIndex + 1 - pageStart;
        String info = String.format("PAGE  id: %02d  rows: %03d  start-index: %04d  full-length: %d bytes", pageId, nextRowId, pageStart, pageBytes);
        if (nextRowId != buffer.get(lastByteIndex)) {
            return "INCONSISTENT " + info;
        }
        return info;
    }

}
