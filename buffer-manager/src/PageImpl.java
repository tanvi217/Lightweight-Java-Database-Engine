import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import java.nio.ByteBuffer;



public class PageImpl implements Page {
    private static final int PAGE_SIZE = 4096; // Page size in bytes
    public BufferManagerLRU bufferManager;
    public static final int MOVIE_ID_SIZE = 9;  // movieId size: 9
    public static final int TITLE_SIZE = 30;    // title size: 30
    public static final int ROW_SIZE = MOVIE_ID_SIZE + TITLE_SIZE; // each row size
    private int numRows;
    private final int pageId; //page number
    //private final List<Row> rows; //list of rows in each page
    private int currentSize; // current size of the page
    private final ByteBuffer data;  // hold binary data of the page

    public PageImpl(int pageId) {
        this.pageId = pageId;
        this.numRows =0;
        //this.rows = new ArrayList<>();
        this.currentSize = 0; // Starts with an empty page
        
        this.data = ByteBuffer.allocate(PAGE_SIZE);  // ByteBuffer of 4KB  to hold the page data
    }

    //return row by rowId
    @Override
    public Row getRow(int rowId) {
        if (rowId < 0 || rowId*ROW_SIZE >= data.position()) {
            return null;
        }
        //otherwise we continue and extract the row from data
        // Calculate the starting position of the row based on rowIndex
    int position = rowId * ROW_SIZE;

    // Save the current position to restore later
    int currentPosition = data.position();

    // Set the buffer’s position to the start of the desired row
    data.position(position);

    // Create an array to hold the row data
    byte[] rowData = new byte[ROW_SIZE];

    // Read the row into the array
    data.get(rowData);

    // Restore the buffer’s position to where it was before
    data.position(currentPosition);

    // Extract movieId and title from rowData
    byte[] movieId = new byte[MOVIE_ID_SIZE];
    byte[] title = new byte[TITLE_SIZE];

    // Copy the movieId and title from rowData
    System.arraycopy(rowData, 0, movieId, 0, MOVIE_ID_SIZE);  // First MOVIE_ID_SIZE bytes
    System.arraycopy(rowData, MOVIE_ID_SIZE, title, 0, TITLE_SIZE);  // Next TITLE_SIZE bytes

    // Now we have movieId and title, and we can create a row object to return.
    return new Row(movieId, title);

        
    //return rows.get(rowId);
    }

@Override
public int insertRow(Row row) {
    byte[] serializedRow = row.serialize();

    // Check if the current page is full
    if (currentSize + serializedRow.length > PAGE_SIZE) {
        System.out.println("Page is full, creating a new page...");

        
            // Call createPage to get a new page from the buffer manager
            Page newPage = bufferManager.createPage();  

            
            newPage.insertRow(row);  // Insert the row in the new page

            return 0;  
        
            
    }

    // If there's space in the current page, insert the row there
    data.put(serializedRow);  // Insert the row data into the current page
    currentSize += serializedRow.length;
    numRows++;  // Increment the number of rows inserted

    System.out.println("Inserted row into current page. Row count: " + numRows);

    return numRows - 1;  // Return the rowId (index)
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
public boolean deserialize(byte[] dataBytes) {
    // Trim trailing zeros, need to do so otherwise we can't write to an empty page.
    byte[] trimmedData = trimTrailingZeros(dataBytes);
    if (currentSize + trimmedData.length > PAGE_SIZE) {
        return false;
    }
    //otherwise there is room so...
    // Wrap the trimmed byte array into ByteBuffer
    ByteBuffer buffer = ByteBuffer.wrap(trimmedData);
    
    // Create PageImpl and put the data into the Page
    //PageImpl page = new PageImpl(this.pageId);
    // put the data into the current buffer and adjust other variables as needed
    data.put(buffer);
    currentSize += trimmedData.length;
    numRows += trimmedData.length/ROW_SIZE;
    return true;
}
// Helper method to remove trailing zeros
private byte[] trimTrailingZeros(byte[] dataBytes) {
    //trims but if last row has trailing 0's for padding reasons keeps them
    //actually more accurately we add back in the necessary trailing 0's after
    int length = dataBytes.length;
    while (length > 0 && dataBytes[length - 1] == 0) {
        length--;
    }
    while(length % ROW_SIZE!=0){
        length++;
    }
    // Create a new array with the valid data (without trailing zeros)
    return Arrays.copyOf(dataBytes, length);
}
}
