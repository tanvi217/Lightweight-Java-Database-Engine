import java.nio.charset.StandardCharsets;

public class basicTest {
    public static void main(String[] args) {
        Row r = new Row(pad("This is a test ID", Constants.MOVIE_ID_SIZE), pad("This is a test title", Constants.TITLE_SIZE));
        System.out.println(r.toString());
        byte[] temp = r.getAttribute(Constants.MOVIE_ID_SIZE, Constants.TITLE_SIZE);
        String temp2 = new String(temp);
        System.out.println(temp2);
        LRUBufferManager bm = new LRUBufferManager(20);
        //System.out.println("Episode 1".compareTo("Episode 20"));
        //basic test to see if scan works on some dummy data. We just print out values and expect it to give us all of the dummy data we created
        scanTest(bm);
        //selects movies with "dummy data" espisodes between 10 and 20. This also returns Episode 2 which is NOT an error since that is how string comparisons are suppsoed to work, although unintuitive 
        selectionMovieTest(bm);
        //tests the selection on something similar to a fake workedon table. again printing out results and making sure we get all results where the category is director, notice here it is every 10 id numbers
        selectionOperatorTest(bm);
        //tests the projection one the result of the selection above. We are making sure we get the same Pids and Mids but we now have rows without the category
        projectionOperatorTest(bm);
        //finally doing a test on the join, similar to the first block join we will do in our final query executor. We make sure that we are only joining on equal movieIds
        joinTest(bm);
        //join2Test(bm);

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
        scanOp.close();
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
        int[] attributeToSelect = {9, 39};
        SelectionOperator selectionOp = new SelectionOperator(scanOp,attributeToSelect , start_range, end_range);
        selectionOp.open();
        Row row;
        while ((row = selectionOp.next()) != null) {
            String id = row.getString(0, 9).trim();
            String title = row.getString(9, 39).trim();
            System.out.println("ID: " + id + " | Title: " + title);
        }
        selectionOp.close();
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
            //System.out.println(movieRow.toString());
            p.insertRow(movieRow);
        }
        ScanOperator scanOp = new ScanOperator(bm, "TestFile3", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
        int[] attributeToSelect = {19, 39};
        SelectionOperator selectionOp = new SelectionOperator(scanOp, attributeToSelect, new String(pad("director", 20)));
        selectionOp.open();
        Row row;
        while ((row = selectionOp.next()) != null) {
            String id = row.getString(0, Constants.WORKEDON_MOVIEID_SIZE).trim();
            String pid = row.getString(Constants.WORKEDON_MOVIEID_SIZE, Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE).trim();
            String category = row.getString(Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE, Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
            System.out.println("ID: " + id + " | Pid: " + pid + " | Category: " + category);
        }
        //should output every 10 id should have category director, looks good yay.
        selectionOp.close();
    }

 
    private static void projectionOperatorTest(LRUBufferManager bm){
         // rows of format Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE
         Page p = bm.createPage("TestFile4", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
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
         ScanOperator scanOp = new ScanOperator(bm, "TestFile4", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
         int[] attributeToSelect = {19, 39};
         SelectionOperator selectionOp = new SelectionOperator(scanOp, attributeToSelect, new String(pad("director", 20)));
         int[][] srcRang = {{0, 9} , {9, 19}};
         ProjectionOperator projOp = new ProjectionOperator(selectionOp, bm, srcRang);
         projOp.open();
         Row row;
         while ((row = projOp.next()) != null) {
             /*System.out.println(row.toString());*/
             String id = row.getString(0, Constants.WORKEDON_MOVIEID_SIZE).trim();
             String pid = row.getString(Constants.WORKEDON_MOVIEID_SIZE, Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE).trim();
             System.out.println("ID: " + id + " | Pid: " + pid);
             System.out.println(row.length());
         }
         //should output every 10 id should have category director, and therefore be present here and the length should be 19 since only mid and pid
         projOp.close();
    }



    private static void joinTest(LRUBufferManager bm){
        Page p = bm.createPage("joinTestFile1", 39);
        for (int i = 0; i < 100; i++) {
            String idStr = "m" + i;
            String titleStr = "Episode " + i;
            byte[] movieId = pad(idStr, 9);
            byte[] title = pad(titleStr, 30);
            Row movieRow = new Row(movieId, title);
            p.insertRow(movieRow);
        }
        ScanOperator scanOpLeft = new ScanOperator(bm, "joinTestFile1", 39);
        //above is creating the scanOp to be used to test selectionMovie
        String start_range = new String(pad("Episode 10", Constants.IMDB_TITLE_SIZE));
        String end_range = new String(pad("Episode 20", Constants.IMDB_TITLE_SIZE));
        int[] attributeToSelect1 = {9, 39};
        SelectionOperator selectionOpLeft = new SelectionOperator(scanOpLeft, attributeToSelect1, start_range, end_range);
        // rows of format Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE
        p = bm.createPage("joinTestFile2", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
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
        ScanOperator scanOpRight = new ScanOperator(bm, "joinTestFile2", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
        int[] attributeToSelect2 = {19, 39};
        SelectionOperator selectionOpRight = new SelectionOperator(scanOpRight, attributeToSelect2, new String(pad("director", 20)));
        int[][] srcRang = {{0, 9} , {9, 19}};
        ProjectionOperator projOpRight = new ProjectionOperator(selectionOpRight, bm, srcRang);
        int[] outAttr = {0, 9};
        int[] inAttr = {0, 9};
        //now we have all the tools necessary for join1
        BNLJoinOperator j = new BNLJoinOperator(selectionOpLeft, projOpRight, outAttr, inAttr, bm, 1);
        j.open();
        Row row;
        while ((row = j.next()) != null) {
            String id = row.getString(0, Constants.WORKEDON_MOVIEID_SIZE).trim();
            String title = row.getString(9, 39).trim();
            String pid = row.getString(Constants.WORKEDON_MOVIEID_SIZE+30, Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE+30).trim();
            System.out.println("ID: " + id + "| Title: " + title + " | Pid: " + pid);
            System.out.println(row.length());
        }
        //should output every 10 id should have category director, and therefore be present here and the length should be 19 since only mid and pid
        j.close();
   }
/* 
    private static void join2Test(LRUBufferManager bm){
        Page p = bm.createPage("join2TestFile1", 39);
        for (int i = 0; i < 100; i++) {
            String idStr = "m" + i;
            String titleStr = "Episode " + i;
            byte[] movieId = pad(idStr, 9);
            byte[] title = pad(titleStr, 30);
            Row movieRow = new Row(movieId, title);
            p.insertRow(movieRow);
        }
        ScanOperator scanOpLeft = new ScanOperator(bm, "join2TestFile1", 39);
        //above is creating the scanOp to be used to test selectionMovie
        String start_range = new String(pad("Episode 10", Constants.IMDB_TITLE_SIZE));
        String end_range = new String(pad("Episode 20", Constants.IMDB_TITLE_SIZE));
        selectionMovie selectionOpLeft = new selectionMovie(scanOpLeft, start_range, end_range);
        // rows of format Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE
        p = bm.createPage("join2TestFile2", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
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
        ScanOperator scanOpRight = new ScanOperator(bm, "join2TestFile2", Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE + Constants.WORKEDON_CATEGORY_SIZE);
        SelectionOperator selectionOpRight = new SelectionOperator(scanOpRight);
        ProjectionOperator projOpRight = new ProjectionOperator(selectionOpRight, bm);
        //now we have all the tools necessary for join1
        join1 j = new join1(selectionOpLeft, projOpRight, bm);
        p = bm.createPage("join2TestFile3", 115);
        for (int i = 0; i < 21; i++) {
            String pIdStr = "p" + i;
            String nameStr = "Name " + i;
            byte[] personId = pad(pIdStr,Constants.WORKEDON_PERSONID_SIZE);
            byte[] name = pad(nameStr, Constants.PERSON_NAME_SIZE);
            Row movieRow = new Row(personId, name);
            p.insertRow(movieRow);
        }
        ScanOperator scanOpRight2 = new ScanOperator(bm, "join2TestFile3", 115);
        join2 j2 = new join2(j, scanOpRight2, bm);
        j2.open();
        Row row;
        while ((row = j2.next()) != null) {
            String id = row.getString(0, Constants.WORKEDON_MOVIEID_SIZE).trim();
            String title = row.getString(9, 39).trim();
            String pid = row.getString(Constants.WORKEDON_MOVIEID_SIZE+30, Constants.WORKEDON_MOVIEID_SIZE + Constants.WORKEDON_PERSONID_SIZE+30).trim();
            System.out.println("ID: " + id + "| Title: " + title + " | Pid: " + pid);
            System.out.println(row.length());
        }
        //should output 2 results
    }
     */
}
