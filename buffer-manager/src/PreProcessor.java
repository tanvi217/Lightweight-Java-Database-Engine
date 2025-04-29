import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

public class PreProcessor {
    public static void main(String[] args) throws Exception {
        BufferManager bm = new LRUBufferManager(250);

        System.out.println("Loading tables...");
        loadMovies(bm, "data//title.basics.tsv", true, false);
        //loadMovies(bm, Constants.IMDB_TSV_FILE, true, false);
        loadWorkedOn(bm, Constants.IMDB_WORKED_ON_TSV_FILE, true); // TODO: remove limitProcessingForDebug
        loadPeople(bm, Constants.IMDB_PEOPLE_TSV_FILE, false);

        System.out.println("Pre processing done.");
    }

    private static byte[] pad(String s, int len) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        byte[] b2 = new byte[len];
        System.arraycopy(b, 0, b2, 0, Math.min(b.length, len));

        return b2;
    }

    private static int loadMovies(BufferManager bm, String path, boolean skipLongMovieId, boolean limitProcessingForDebug) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(path));
        br.readLine(); // skip header
        int rows = 0, pages = 0, skipped = 0;
        int rowLen = Constants.MOVIE_ID_SIZE + Constants.TITLE_SIZE; // 9 + 30 + 0 = 39 bytes
        Page page = bm.createPage("Movies", rowLen);
        String line;
        Deque<Row> lastFive = new ArrayDeque<>(5);

        while ((line = br.readLine()) != null) {
            if (limitProcessingForDebug && page.getId() > 500000) break;

            String[] t = line.split("\t");
            byte[] idB = t[0].getBytes();

            if (skipLongMovieId && idB.length > Constants.MOVIE_ID_SIZE) { skipped++; continue; }

            if (page.isFull()) {
                bm.unpinPage(page.getId(), "Movies");
                page = bm.createPage("Movies", rowLen);
                pages++;
            }

            Row r = new Row(pad(t[Constants.MOVIE_ID_INDEX], Constants.MOVIE_ID_SIZE), pad(t[2], Constants.TITLE_SIZE));
            if (page.insertRow(r) >= 0) {
                rows++;
                bm.markDirty(page.getId(), "Movies");

                lastFive.addLast(r);
                if (lastFive.size() > 5) lastFive.removeFirst();
            }
        }

        bm.unpinPage(page.getId(), "Movies");
        br.close();
        bm.force();
        System.out.printf("Movies: processed %d, skipped %d, pages %d%n", rows, skipped, pages);

        System.out.println("Last 5 Movies rows:");
        for (Row r : lastFive) System.out.println(r.toString());
        for(Row r : lastFive){
            System.out.println(r.getAttribute(skipped, rowLen));
        }
        return rows;
    }
    
    private static int loadWorkedOn(BufferManager bm, String path, boolean limitProcessingForDebug) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(path));
        br.readLine(); // skip header
        int rows = 0, pages = 0;
        int rowLen = Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE;
        Page page = bm.createPage("WorkedOn", rowLen);
        String line;
        Deque<Row> lastFive = new ArrayDeque<>(5);

        while ((line = br.readLine()) != null) {
            if (limitProcessingForDebug && page.getId() > 500000) break; // limit to 500000 pages for testing

            String[] t = line.split("\t");

            if (page.isFull()) {
                bm.unpinPage(page.getId(), "WorkedOn");
                page = bm.createPage("WorkedOn", rowLen);
                pages++;
            }

            Row r = new Row(
                pad(t[0], Constants.WORKEDON_MOVIEID_SIZE),
                pad(t[2], Constants.WORKEDON_PERSONID_SIZE),
                pad(t[3], Constants.WORKEDON_CATEGORY_SIZE)
            );

            if (page.insertRow(r) >= 0) {
                rows++;
                bm.markDirty(page.getId(), "WorkedOn");
                lastFive.addLast(r);
                if (lastFive.size() > 5) lastFive.removeFirst();
            }
        }

        bm.unpinPage(page.getId(), "WorkedOn");
        bm.force();
        br.close();

        System.out.printf("\nWorkedOn: processed %d rows, pages %d%n", rows, pages);
        System.out.println("Last 5 WorkedOn rows:");
        for (Row r : lastFive) System.out.println(r.toString());

        return rows;
    }

    private static int loadPeople(BufferManager bm, String path, boolean limitProcessingForDebug) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(path));
        br.readLine(); // skip header
        int rows = 0, pages = 0;
        int rowLen = Constants.PERSON_ID_SIZE + Constants.PERSON_NAME_SIZE;
        Page page = bm.createPage("People", rowLen);
        String line;
        Deque<Row> lastFive = new ArrayDeque<>(5);

        while ((line = br.readLine()) != null) {
            if (limitProcessingForDebug && pages > 200) break; // limit to 200 pages for testing

            String[] t = line.split("\t");
            if (page.isFull()) {
                bm.unpinPage(page.getId(), "People");
                page = bm.createPage("People", rowLen);
                pages++;
            }

            Row r = new Row(pad(t[0], Constants.PERSON_ID_SIZE), pad(t[1], Constants.PERSON_NAME_SIZE));
            if (page.insertRow(r) >= 0) {
                rows++;
                bm.markDirty(page.getId(), "People");
                lastFive.addLast(r);
                if (lastFive.size() > 5) lastFive.removeFirst();
            }
        }

        bm.unpinPage(page.getId(), "People");
        bm.force();
        br.close();
        System.out.printf("\nPeople: processed %d rows, pages %d%n", rows, pages);
        System.out.println("Last 5 People rows:");
        for (Row r : lastFive) System.out.println(r.toString());

        return rows;
    }
}