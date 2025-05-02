import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class RelationTest {

    private static int numRows = 1000;
    private BufferManager bm;
    private Relation movies;
    private Relation workedOn;
    private Relation people;

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
    private Row getTestRow(Relation sch, int pid, int sid) {
        ByteBuffer dataBuffer = ByteBuffer.allocate(sch.bytesInRow);
        for (int i = 0; i < sch.attrRanges.length; ++i) {
            int[] range = sch.attrRanges[i];
            dataBuffer.position(range[0]);
            String filler = randomString(range[1] - range[0] - 9); // number of bytes before 8 byte suffix
            dataBuffer.put(filler.getBytes(StandardCharsets.UTF_8));
            dataBuffer.putInt(pid);
            dataBuffer.putInt(sid);
            dataBuffer.put((byte) ('0' + i));
        }
        dataBuffer.clear();
        return new Row(dataBuffer);
    }

    // adds enough pages to hold the specified number of rows
    private void populateTestRelation(Relation sch) {
        Page p = sch.createPage();
        int sid = 0;
        for (int i = 0; i < numRows; ++i) {
            if (p.isFull()) {
                sch.unpinPage(p.getId());
                p = sch.createPage();
                sid = 0;
            }
            p.insertRow(getTestRow(sch, p.getId(), sid));
            ++sid;
        }
        sch.unpinPage(p.getId());
    }

    @Before
    public void initializeRelations() { // runs before each test case
        bm = new LRUBufferManager(40);
        movies = new Movies(bm, true);
        workedOn = new WorkedOn(bm, true);
        people = new People(bm, true);
        populateTestRelation(movies);
        populateTestRelation(workedOn);
        populateTestRelation(people);
    }

    @Test
    public void testBTree() {
        BTree<String> bt = new ByteKeyBTree<>(20, bm, true); // (bm, WorkedOn.category);
        int pid = 0;
        int sid = 0;
        Rid testRid = new Rid(-30, -45); // fake Rid with invalid arguments
        String testKey = "TestRid1";
        bt.insert(testKey, testRid);
        Page p = workedOn.getPage(pid);
        for (int i = 0; i < numRows; ++i) {
            if (sid >= p.height()) {
                workedOn.unpinPage(pid);
                ++pid;
                p = workedOn.getPage(pid);
                sid = 0;
            }
            Row row = p.getRow(sid);
            String passingKey = row.getString(WorkedOn.category);
            bt.insert(passingKey, new Rid(pid, sid));
            ++sid;
        }
        workedOn.unpinPage(pid);
        Iterator<Rid> result = bt.search(testKey);
        assertTrue("Search should find previously inserted Rid.", result.hasNext());
        assertEquals("Result should contain previously inserted page id.", testRid.getPageId(), result.next().getPageId());
        result = bt.search("Fake" + testKey);
        assertFalse("Searching for key which was not inserted should lead to zero results.", result.hasNext());
        result = bt.rangeSearch("g", "gg");
        while (result.hasNext()) {
            Rid rid = result.next();
            int pageId = rid.getPageId();
            if (pageId < 0) {
                continue; // found fake pageId from testRid
            }
            Page target = workedOn.getPage(pageId);
            Row row = target.getRow(rid.getSlotId());
            String category = row.getString(WorkedOn.category);
            ByteBuffer nineBytes = row.getRange(WorkedOn.movieId);
            assertEquals("Test row should contain page id", pageId, nineBytes.getInt());
            assertEquals("Test row should contain slot id", rid.getSlotId(), nineBytes.getInt());
            System.out.println(category); // printing all random strings that were alphabetically between "g" and "gg"
            workedOn.unpinPage(target.getId());
        }
    }

}
