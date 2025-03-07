/* 
import java.nio.ByteBuffer;
import java.util.Arrays;

public class SizedPage implements Page {

    private int rowSize;
    private int totalRows;
    private int fullRows;
    private int pageId;
    private Row[] rows;

    public SizedPage(int pageBytes, int rowBytes, int id) {
        rowSize = rowBytes;
        totalRows = pageBytes / rowSize;
        fullRows = 0;
        pageId = id;
        rows = new Row[totalRows];
    }

    public SizedPage(int id) {
        this(4096, 39, id);
    }

    @Override
    public Row getRow(int rowId) {
        return rows[rowId];
    }

    @Override
    public int insertRow(Row row) {
        if (fullRows >= totalRows) {
            return -1;
        }
        rows[fullRows] = row;
        return fullRows++;
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
    public byte[] serialize(int numRows) {
        if (numRows > totalRows) {
            throw new IllegalArgumentException("Cannot serialize more than " + totalRows + " rows.");
        }
        ByteBuffer data = ByteBuffer.allocate(rowSize * numRows);
        for (int i = 0; i < numRows; ++i) {
            data.put(rows[i].movieId);
            data.put(rows[i].title);
        }
        return data.array();
    }

    @Override
    public void deserialize(byte[] data) {
        if (data.length > rowSize * totalRows) {
            throw new IllegalArgumentException("Data to deserialize exceeds page size.");
        }
        int numRows = data.length / rowSize;
        for (int i = 0; i < numRows; ++i) {
            int rowStart = i * rowSize;
            byte[] movieId = Arrays.copyOfRange(data, rowStart, rowStart + 9);
            byte[] title = Arrays.copyOfRange(data, rowStart + 9, rowStart + 39);
            rows[i] = new Row(movieId, title);
        }
    }

}
    */
