import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

public class TabularPageTest {

    private static Row intRow(int content) {
        return new Row(ByteBuffer.allocate(4).putInt(content).clear());
    }

    @Test
    public void testInsertRow() {
        ByteBuffer bb = ByteBuffer.allocate(56); // larger than pageStart + pageLength
        int pageStart = 12;
        int pageLength = 40;
        Page p = new TabularPage(0, pageStart, pageLength, bb, 4);
        for (int i = 0; i < 3; ++i) {
            p.insertRow(intRow(i));
        }
        bb.clear();
        bb.position(pageStart);
        for (int i = 0; i < 3; ++i) {
            assertEquals("The ith row should be the integer i", i, bb.getInt());
        }
        for (int i = 0; i < 3; ++i) {
            p.insertRow(intRow(i), 1);
        }
        int[] expected = {0, 2, 1, 0, 1, 2};
        bb.clear();
        bb.position(pageStart);
        for (int i = 0; i < 6; ++i) {
            assertEquals("The ith row should match expected.", expected[i], bb.getInt());
        }
    }

}
