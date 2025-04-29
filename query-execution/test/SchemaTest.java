import java.nio.ByteBuffer;
import org.junit.Before;

public class SchemaTest {

    private Schema movies;
    private Schema workedOn;
    private Schema people;

    // used to generate a row for testing purposes.
    // assumes all attributes are at least 9 bytes
    private Row getTestRow(Schema sch, int pid, int rid) {
        ByteBuffer dataBuffer = ByteBuffer.allocate(sch.length);
        for (int i = 0; i < sch.ranges.length; ++i) {
            int[] range = sch.ranges[i];
            dataBuffer.position(range[0]);
            dataBuffer.put((byte) i);
            dataBuffer.putInt(rid);
            dataBuffer.putInt(pid);
        }
        return new Row(dataBuffer);
    }

    // adds enough pages to hold the specified number of rows
    private void populateTestRelation(Schema sch, int numRows) {
        Page p = sch.createPage();
        for (int i = 0; i < numRows; ++i) {
            if (p.isFull()) {
                sch.unpinPage(p.getId());
                p = sch.createPage();
            }
            p.insertRow(getTestRow(sch, p.getId(), i));
        }
        sch.unpinPage(p.getId());
    }

    @Before
    public void initializeRelations() { // runs before each test case
        BufferManager bm = new LRUBufferManager(40);
        movies = new Movies(bm);
        workedOn = new WorkedOn(bm);
        people = new People(bm);
        int numRows = 1000;
        populateTestRelation(movies, numRows);
        populateTestRelation(workedOn, numRows);
        populateTestRelation(people, numRows);
    }

}
