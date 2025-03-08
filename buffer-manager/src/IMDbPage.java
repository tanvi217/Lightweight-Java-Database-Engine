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

    private void setNextRowId(int rowId) {
        nextRowId = rowId;
        buffer.put(lastByteIndex, (byte) rowId);
    }
    
    public IMDbPage(int pageId, int pageBytes, ByteBuffer buffer, boolean isEmpty) {
        this.pageId = pageId;
        this.buffer = buffer;
        maxRows = (pageBytes - 1) / rowBytes; // subtract one for nextRowId byte
        pageStart = pageId * pageBytes;
        lastByteIndex = pageStart + pageBytes - 1;
        rows = new Row[maxRows];
        if (isEmpty) {
            setNextRowId(0);
        } else {
            nextRowId = (int) buffer.get(lastByteIndex);
        }
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
        buffer.get(movieId, rowStart, rowStart + 9);
        buffer.get(title, rowStart + 9, rowStart + 39);
        rows[rowId] = new Row(movieId, title);
        return rows[rowId];
    }

    @Override
    public int insertRow(Row row) {
        if (nextRowId >= maxRows) {
            return -1; // I think it said to do this in project description
        }
        int rowId = nextRowId;
        int rowStart = pageStart + rowId * rowBytes;
        buffer.put(row.movieId, rowStart, rowStart + 9);
        buffer.put(row.title, rowStart + 9, rowStart + 39);
        rows[rowId] = row;
        setNextRowId(nextRowId + 1);
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

}
