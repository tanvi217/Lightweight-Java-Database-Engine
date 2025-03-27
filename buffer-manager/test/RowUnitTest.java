import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import java.nio.charset.StandardCharsets;

public class RowUnitTest {

    private Row row;
    private byte[] data;

    @Before
    public void setUp() {
        data = "123456789Example Movie".getBytes(StandardCharsets.UTF_8);
        row = new Row(data);
    }

    @Test
    public void testConstructor() {
        //tests that constructor properly saves values.
        assertArrayEquals("Row data should be correctly initialized", data, row.data);
    }

    @Test
    public void testGetData() {
        //tests that get movieId returns proper string.
        assertEquals("Full row data should be correctly retrieved", "123456789Example Movie", row.toString());
    }

}
