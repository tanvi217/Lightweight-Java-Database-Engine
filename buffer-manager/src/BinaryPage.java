import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

public class BinaryPage implements Page {
    private int pid;
    private List<Row> rows;

    public BinaryPage(int pid) {
        this.pid = pid;
        this.rows = new ArrayList<>();
    }

    @Override
    public int getId() {
        return pid;
    }

    @Override
    public boolean isFull() {
        // 4 bytes for row count, then 39 bytes per row
        return rows.size() >= Constants.MAX_ROWS;
    }

    @Override
    public int insertRow(Row row) {
        if (4 + (rows.size() * Constants.ROW_SIZE) + Constants.ROW_SIZE <= Constants.PAGE_SIZE) {
            rows.add(row);
            return rows.size(); // Return the index of the inserted row
        }

        return -1;
    }

    @Override
    public Row getRow(int index) {
        return (index >= 0 && index < rows.size()) ? rows.get(index) : null;
    }

    @Override
    // Serialize page to byte array
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Constants.PAGE_SIZE);
        
        int rowCount = rows.size();

        if (4 + rowCount * Constants.ROW_SIZE > Constants.PAGE_SIZE) {
            throw new IllegalStateException("Too many rows: " + rowCount);
        }

        buffer.putInt(rowCount);

        for (Row row : rows) {
            byte[] serializedRow = row.serialize();
            buffer.put(serializedRow);
        }
        
        while (buffer.hasRemaining()) {
            buffer.put((byte) 0);
        }

        return buffer.array();
    }

    @Override
    // Deserialize byte array into this Page, return true if successful
    public boolean deserialize(byte[] data) {
        if (data == null || data.length != Constants.PAGE_SIZE) {
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        int rowCount = buffer.getInt();
        
        if (rowCount < 0 || rowCount > (Constants.PAGE_SIZE - 4) / Constants.ROW_SIZE) {
            return false;
        }

        rows.clear();

        try {
            for (int i = 0; i < rowCount; i++) {
                byte[] rowData = new byte[Constants.ROW_SIZE];
                buffer.get(rowData);
                rows.add(Row.deserialize(rowData));
            }

            return true;
        } catch (Exception e) {
            rows.clear();

            return false;
        }
    }
}