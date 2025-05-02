import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

public class PreProcessor {
    public static void main(String[] args) throws Exception {
        // if (args.length != 3) {
        //     System.err.println("Usage: run_query <start_range> <end_range> <buffer_size>");
        //     return;
        // }

        // String start = args[0];
        // String end   = args[1];
        // int bufSize  = Integer.parseInt(args[2]);

        BufferManager bm = new LRUBufferManager(250);
        boolean debugPrinting = true; // Set to false to disable debug printing

        System.out.println("Loading tables...");
        loadMovies(bm, Constants.IMDB_TSV_FILE, true, false);
        loadWorkedOn(bm, Constants.IMDB_WORKED_ON_TSV_FILE, true); // TODO: remove limitProcessingForDebug
        loadPeople(bm, Constants.IMDB_PEOPLE_TSV_FILE, false);
        System.out.println("Pre processing done.");

        Operator movieScan = new ScanOperator(bm, "Movies", Constants.MOVIE_ID_SIZE + Constants.TITLE_SIZE);
        Operator movieSel  = new selectionMovie(movieScan, "Episode #3.17", "Episode #3.20"); // todo : check if the operator works correctly

        if (debugPrinting) {
            movieSel.open();
            System.out.println("--- Raw Movies (first 5 rows) ---");
            Row dbgRaw;
            int rawCount = 0;

            while ((dbgRaw = movieSel.next()) != null && rawCount < 5) {
                String combined = new String(dbgRaw.getAttribute(0, Constants.MOVIE_ID_SIZE + Constants.TITLE_SIZE), StandardCharsets.UTF_8).trim();
                System.out.println(combined);
                rawCount++;
            }

            movieSel.close();
        }        

        Operator workedScan = new ScanOperator(bm, "WorkedOn", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
        Operator workedSel  = new selectionMovie(workedScan, "director", "director");
        Operator workedProj = new ProjectionOperator(workedSel, bm);

        Operator join1 = new join1(movieSel, workedProj, bm);

        Operator peopleScan = new ScanOperator(bm, "People", Constants.PERSON_ID_SIZE + Constants.PERSON_NAME_SIZE);
        Operator join2 = new join1(join1, peopleScan, bm);

        join2.open();
        Row out;

        while ((out = join2.next()) != null) {
            String title = new String(out.getAttribute(0, Constants.MOVIE_ID_SIZE + Constants.TITLE_SIZE), StandardCharsets.UTF_8).trim();
            int nameOff = Constants.MOVIE_ID_SIZE + Constants.TITLE_SIZE + Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.PERSON_ID_SIZE;
            String name = new String(out.getAttribute(nameOff, Constants.PERSON_NAME_SIZE), StandardCharsets.UTF_8).trim();
            System.out.println(title + "," + name);
        }
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