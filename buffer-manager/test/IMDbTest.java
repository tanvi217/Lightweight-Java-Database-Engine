import java.io.EOFException;
import java.io.IOException;

/**
 * Comments are intended to give an idea of what the buffer might look like, but
 * the real contents could differ depending on implementation.
 */
public class IMDbTest {

    public static String datasetTest() {
        int bufferSize = 8;
        BufferManager bm = Test.getNewBM(bufferSize);
        Utilities.loadDataset(bm, Constants.IMDB_TSV_FILE);
        System.out.println(bm);
        return "Pass";
    }

}
