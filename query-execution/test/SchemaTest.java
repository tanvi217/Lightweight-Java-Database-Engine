import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class SchemaTest {

    private static int numRows = 100;
    private BufferManager bm;
    private Schema movies;
    private Schema workedOn;
    private Schema people;

    public static String randomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; ++i) {
            char next = (char) ('a' + random.nextInt(26));
            sb.append(next);
        }
        return sb.toString();
    }

    // used to generate a row for testing purposes.
    // assumes all attributes are at least 9 bytes
    private Row getTestRow(Schema sch, int pid, int rid) {
        ByteBuffer dataBuffer = ByteBuffer.allocate(sch.length);
        for (int i = 0; i < sch.ranges.length; ++i) {
            int[] range = sch.ranges[i];
            dataBuffer.position(range[0]);
            dataBuffer.put((byte) ((char) i));
            String filler = randomString(range[1] - range[0] - 9); // number of bytes before 8 byte suffix
            dataBuffer.put(filler.getBytes(StandardCharsets.UTF_8));
            dataBuffer.putInt(rid);
            dataBuffer.putInt(pid);
        }
        return new Row(dataBuffer);
    }

    // adds enough pages to hold the specified number of rows
    private void populateTestRelation(Schema sch) {
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
        bm = new LRUBufferManager(40);
        movies = new Movies(bm);
        workedOn = new WorkedOn(bm);
        people = new People(bm);
        populateTestRelation(movies);
        populateTestRelation(workedOn);
        populateTestRelation(people);
    }

    @Test
    public void testBTree() {
        int bytesInKey = WorkedOn.category[1];
        BTree<String> bt = new TempBufferBTree<>(bm, bytesInKey);
        int pid = 0;
        int sid = 0;
        Page p = workedOn.getPage(pid);
        for (int i = 0; i < numRows; ++i) {
            if (sid >= p.height()) {
                workedOn.unpinPage(pid);
                ++pid;
                p = workedOn.getPage(pid);
                sid = 0;
            }
            Row row = p.getRow(sid);
            bt.insert(row.getString(WorkedOn.category), new Rid(pid, sid));
            ++sid;
        }
        Iterator<Rid> result = bt.rangeSearch("2b", "2c");
        while (result.hasNext()) {
            Rid rid = result.next();
            Page target = workedOn.getPage(rid.getPageId());
            String category = target.getRow(rid.getSlotId()).getString(WorkedOn.category);
            System.out.println(category);
            workedOn.unpinPage(target.getId());
        }
        result = bt.search("1");
        assertFalse("h", result.hasNext());
    }

}
