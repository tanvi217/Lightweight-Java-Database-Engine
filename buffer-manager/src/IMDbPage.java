import java.nio.ByteBuffer;

public class IMDbPage implements Page {

    private static int rowBytes = 39;
    private int totalRows;
    private int pageStart; // index of first byte of this page in buffer
    private ByteBuffer buffer;
    private int fullRows;
    private int pageId;
    private Row[] rows;

    public IMDbPage(int pageId, int pageBytes, ByteBuffer buffer) {
        this.buffer = buffer;
        this.pageId = pageId;
        totalRows = pageBytes / rowBytes;
        pageStart = pageId * pageBytes;
        fullRows = 0;
        rows = new Row[totalRows];
    }

    @Override
    public Row getRow(int rowId) {
        if (rowId < 0 || rowId >= fullRows) {
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
        if (fullRows >= totalRows) {
            return -1;
        }
        int rowId = fullRows;
        int rowStart = pageStart + rowId * rowBytes;
        buffer.put(row.movieId, rowStart, rowStart + 9);
        buffer.put(row.title, rowStart + 9, rowStart + 39);
        rows[rowId] = row;
        fullRows += 1;
        return rowId;
    }

    @Override
    public boolean isFull() {
        return fullRows >= totalRows;
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
