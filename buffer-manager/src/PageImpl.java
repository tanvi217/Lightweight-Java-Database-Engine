import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;


public class PageImpl implements Page {
    private static final int PAGE_SIZE = 4096; // Page size in bytes
    private final int pageId; //page number
    private final List<Row> rows; //list of rows in each page
    private int currentSize; // current size of the page
    private final ByteBuffer data;  // hold binary data of the page

    public PageImpl(int pageId) {
        this.pageId = pageId;
        this.rows = new ArrayList<>();
        this.currentSize = 0; // Starts with an empty page
        this.data = ByteBuffer.allocate(PAGE_SIZE);  // ByteBuffer of 4KB  to hold the page data
    }

    //return row by rowId
    @Override
    public Row getRow(int rowId) {
        if (rowId < 0 || rowId >= rows.size()) {
            return null;
        }
        return rows.get(rowId);
    }

    @Override
public int insertRow(Row row) {
    byte[] serializedRow = row.serialize(); 
    
    if (currentSize + serializedRow.length > PAGE_SIZE) {
        return -1;
    }
    
    

    data.put(serializedRow); 
    rows.add(row); 
    currentSize += serializedRow.length;

    return rows.size() - 1; 
}

@Override
public boolean isFull() {
    // is page full?
    return currentSize >= PAGE_SIZE;
}

@Override
public int getId() {
    return pageId;
}
@Override
public byte[] serialize() {
    return data.array(); 
}

@Override
public Page deserialize(byte[] dataBytes) {
    ByteBuffer buffer = ByteBuffer.wrap(dataBytes);
    PageImpl page = new PageImpl(this.pageId);
    page.data.put(buffer);
    return page;
}

}
