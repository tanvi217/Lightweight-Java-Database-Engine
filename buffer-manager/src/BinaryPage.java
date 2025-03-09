import java.util.*;
import java.nio.ByteBuffer;

public class BinaryPage implements Page {
    private int pid;
    private List<Row> rows;
    private static final int PAGE_SIZE = 4096;
    private static final int ROW_SIZE = 39;    // 9 for movieId + 30 for title
    private static final int MAX_ROWS = (PAGE_SIZE - 4) / ROW_SIZE;

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
        return rows.size() >= MAX_ROWS;
    }

    @Override
    public int insertRow(Row row) {
        if (4 + (rows.size() * ROW_SIZE) + ROW_SIZE <= PAGE_SIZE) {
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
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        
        int rowCount = rows.size();
        
        if (4 + rowCount * ROW_SIZE > PAGE_SIZE) {
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
        if (data == null || data.length != PAGE_SIZE) {
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        int rowCount = buffer.getInt();
        if (rowCount < 0 || rowCount > (PAGE_SIZE - 4) / ROW_SIZE) {
            return false;
        }

        rows.clear();

        try {
            for (int i = 0; i < rowCount; i++) {
                byte[] rowData = new byte[ROW_SIZE];
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