import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

public class BinaryPage implements Page {
    private int pid;
    private List<Row> rows;
    private int recordsCount;

    public BinaryPage(int pid) {
        this.pid = pid;
        this.rows = new ArrayList<>();
        this.recordsCount = 0;
    }

    public BinaryPage(int pid, List<Row> rows) {
        this.pid = pid;
        this.rows = rows;
        this.recordsCount = rows.size();
    }

    @Override
    public int getId() {
        return pid;
    }

    @Override
    public boolean isFull() {
        return rows.size() >= Constants.MAX_ROWS;
    }

    @Override
    public int insertRow(Row row) {
        if (this.recordsCount < Constants.MAX_ROWS) {
            this.recordsCount++;
            rows.add(row);

            return this.recordsCount - 1;
        }

        return -1;
    }

    @Override
    public Row getRow(int rowId) {
        if (rowId < 0 || rowId >= this.recordsCount) {
            return null;
        }

        return rows.get(rowId);
    }
    

    @Override
    public byte[] serialize() {
        ByteBuffer pageBuffer = ByteBuffer.allocate(Constants.PAGE_SIZE);

        int totalSize = 4 + (recordsCount * Constants.ROW_SIZE);
        if (totalSize > Constants.PAGE_SIZE) {
            throw new IllegalStateException("Page overflow: " + recordsCount + " rows");
        }

        pageBuffer.putInt(recordsCount);

        int position = 4; // Start after records count
        int rowIndex = 0;

        while (pageBuffer.hasRemaining()) {
            if (rowIndex < recordsCount) {
                byte[] rowBytes = rows.get(rowIndex).serialize();
                pageBuffer.put(rowBytes);
                position += Constants.ROW_SIZE;
                rowIndex++;
            } else {
                pageBuffer.put((byte) 0x00); // Pad remaining space with zeros
            }
        }

        return pageBuffer.array();
    }

    @Override
    public boolean deserialize(byte[] rawData) {
        if (rawData == null || rawData.length != Constants.PAGE_SIZE) {
            System.err.println("Deserialization failed: Invalid data length or null input");
            return false;
        }

        ByteBuffer dataBuffer = ByteBuffer.wrap(rawData);

        int numEntries = dataBuffer.getInt();
        if (numEntries < 0 || numEntries > Constants.MAX_ROWS) {
            System.err.println("Invalid entry count: " + numEntries);
            return false;
        }

        rows.clear();
        recordsCount = 0;

        try {
            for (int i = 0; i < numEntries; i++) {
                byte[] rowBytes = new byte[Constants.ROW_SIZE];
                dataBuffer.get(rowBytes);
                rows.add(Row.deserialize(rowBytes));
                recordsCount++;
            }

            return true;
        } catch (Exception ex) {
            System.err.println("Failed to deserialize entries: " + ex.getMessage());
            rows.clear();
            recordsCount = 0;

            return false;
        }
    }
}