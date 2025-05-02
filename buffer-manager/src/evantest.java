import java.nio.charset.StandardCharsets;

public class evantest {
    public static void main(String[] args) {
        Row r = new Row(pad("This is a test ID", Constants.MOVIE_ID_SIZE), pad("This is a test title", Constants.TITLE_SIZE));
        System.out.println(r.toString());
        byte[] temp = r.getAttribute(Constants.MOVIE_ID_SIZE, Constants.TITLE_SIZE);
        String temp2 = new String(temp);
        System.out.println(temp2);
        LRUBufferManager bm = new LRUBufferManager(5);
        System.out.println("Episode 1".compareTo("Episode 20"));
        //scanTest(bm);
        //selectionMovieTest(bm);
        selectionOperatorTest(bm);

    }
    private static byte[] pad(String s, int len) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        byte[] b2 = new byte[len];
        System.arraycopy(b, 0, b2, 0, Math.min(b.length, len));

        return b2;
    }

    
    private static void scanTest(LRUBufferManager bm){
        Page p = bm.createPage("TestFile1", 39);
        for (int i = 0; i < 100; i++) {
            String idStr = "m" + i;
            String titleStr = "Episode " + i;
            byte[] movieId = pad(idStr, 9);
            byte[] title = pad(titleStr, 30);
            Row movieRow = new Row(movieId, title);
            p.insertRow(movieRow);
        }
        ScanOperator scanOp = new ScanOperator(bm, "TestFile1", 39);
        scanOp.open();
        Row row;
        while ((row = scanOp.next()) != null) {
            String id = row.getString(0, 9).trim();
            String title = row.getString(9, 39).trim();
            System.out.println("ID: " + id + " | Title: " + title);
        }
    }

    private static void selectionMovieTest(LRUBufferManager bm){
        Page p = bm.createPage("TestFile2", 39);
        for (int i = 0; i < 100; i++) {
            String idStr = "m" + i;
            String titleStr = "Episode " + i;
            byte[] movieId = pad(idStr, 9);
            byte[] title = pad(titleStr, 30);
            Row movieRow = new Row(movieId, title);
            p.insertRow(movieRow);
        }
        ScanOperator scanOp = new ScanOperator(bm, "TestFile2", 39);
        //above is creating the scanOp to be used to test selectionMovie
        String start_range = new String(pad("Episode 10", Constants.IMDB_TITLE_SIZE));
        String end_range = new String(pad("Episode 20", Constants.IMDB_TITLE_SIZE));
        selectionMovie selectionOp = new selectionMovie(scanOp, start_range, end_range);
        selectionOp.open();
        Row row;
        while ((row = selectionOp.next()) != null) {
            String id = row.getString(0, 9).trim();
            String title = row.getString(9, 39).trim();
            System.out.println("ID: " + id + " | Title: " + title);
        }
    }

    private static void selectionOperatorTest(LRUBufferManager bm){
        // rows of format Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE
        Page p = bm.createPage("TestFile3", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
        for (int i = 0; i < 100; i++) {
            String idStr = "m" + i;
            String pIdStr = "p" + i;
            String catStr = "Category " + i;
            if(i%10 == 0){
                catStr = "director";
            }
            byte[] movieId = pad(idStr, Constants.WORKEDON_MOVIEID_SIZE);
            byte[] personId = pad(pIdStr,Constants.WORKEDON_PERSONID_SIZE);
            byte[] category = pad(catStr, Constants.WORKEDON_CATEGORY_SIZE);
            Row movieRow = new Row(movieId, personId, category);
            p.insertRow(movieRow);
        }
        ScanOperator scanOp = new ScanOperator(bm, "TestFile3", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
        SelectionOperator selectionOp = new SelectionOperator(scanOp);
        selectionOp.open();
        Row row;
        while ((row = selectionOp.next()) != null) {
            String id = row.getString(0, Constants.WORKEDON_MOVIEID_SIZE).trim();
            String pid = row.getString(Constants.WORKEDON_MOVIEID_SIZE, Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE).trim();
            String category = row.getString(Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE, Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
            System.out.println("ID: " + id + " | Pid: " + pid + " | Category: " + category);
        }
        //should output every 10 id should have category director, looks good yay.
    }
}
