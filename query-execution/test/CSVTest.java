import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class CSVTest {

    @Test
    public void testRelationsCSV() {
        String tableTitle = "FakeTable";
        BufferManager bm = new LRUBufferManager();
        Relation fakeTable = Relation.retrieveFromRelationsCSV(tableTitle, bm);
        assertEquals("Number of pages in buffer manager should be updated to match relations.csv", 22, bm.getPageCount(tableTitle));
        int bytesInRow = fakeTable.bytesInRow;
        fakeTable.attrRanges[fakeTable.attrRanges.length - 1][1] = bytesInRow + 1; // increment size of last attribute (you shouldn't really be able to do this but it's just for testing purposes)
        fakeTable.saveToRelationsCSV();
        fakeTable = Relation.retrieveFromRelationsCSV(tableTitle, bm); // get new Relation object
        assertEquals("Length of row should have been updated by saving the row to relations.csv", bytesInRow + 1, fakeTable.bytesInRow);
    }

}
