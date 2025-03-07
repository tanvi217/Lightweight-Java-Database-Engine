public class SizedPage implements Page {

    private int numRows;
    private int fullRows;
    private int pageId;
    private Row[] rows;

    public SizedPage(int pageBytes, int rowBytes, int id) {
        numRows = pageBytes / rowBytes;
        fullRows = 0;
        pageId = id;
        rows = new Row[numRows];
    }

    @Override
    public Row getRow(int rowId) {
        return rows[rowId];
    }

    @Override
    public int insertRow(Row row) {
        if (fullRows >= numRows) {
            return -1;
        }
        rows[fullRows] = row;
        return fullRows++;
    }

    @Override
    public boolean isFull() {
        return fullRows >= numRows;
    }

    @Override
    public int getId() {
        return pageId;
    }

}
