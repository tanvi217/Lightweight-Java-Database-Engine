import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        //String IMDB_FILE_PATH = "C:\\Users\\abhis\\Desktop\\Priyanka\\UMass\\Spring25\\645\\lab1\\cs645-labs\\buffer-manager\\data\\title.basics.tsv\\title.basics.tsv"; //change path on your machine, MANUALLY
        //int bufferPoolSize = 3;

        // BufferManager bufferManager = new LRUBufferManager(bufferPoolSize);
        // Utilities.loadDataset(bufferManager, imdbFilePath);
        

        BufferManager bufferManager = new BufferManagerLRU(Constants.BUFFER_SIZE);
        Utilities.loadDataset(bufferManager, Constants.IMDB_FILE_PATH);
        // bufferManagerLRUTests();
    }

    private static void bufferManagerLRUTests(){
        //This test is useful as we run on a small set of pages and make sure the basic functionality works
        //We are able to add pages to the buffer manager, keep them in the frames, evict to add new ones, and write to disk
        //The disk writes are all 0 as each page has not had any information added yet, but importantly in imdb_db.bin we can
        //see that there are 6000 lines (in hex) i.e. 6 pages worth of space. We do this using the hex editor installed on vscode
        int totalPages = 10;
        int bufferSize = 4;
        BufferManager bm = new BufferManagerLRU(bufferSize, true);
        for (int i = 0; i < totalPages; ++i) {
            try {
                Page page = bm.createPage();
                System.out.println("A" + page.getId());
                byte[] id = "tt0000001".getBytes();
                byte[] title = "Carmencita".getBytes();
                Row r = new Row(id, title);
            page.insertRow(r);
                bm.unpinPage(page.getId());
            } catch (IOException e) {
                System.out.println(e);
                break;
            }
            System.out.println("got page " + i + ": " + bm.toString());
        }

        //now after we added some pages let's actually try writing something to them...
        try{
            Page page = bm.getPage(0);
            System.out.println(page.getRow(0));
            
            byte[] id = "tt0000001".getBytes();
            byte[] title = "Carmencita".getBytes();
            Row r = new Row(id, title);
            page.insertRow(r);
            System.out.println(page.getRow(1));
            bm.markDirty(0);
            bm.unpinPage(0);
            bm.createPage();
            bm.createPage();
            bm.createPage();
            bm.createPage();
            //this forces us to write to disk and we see it looks good YAY
        }catch (IOException e){
            System.out.println(e);
        }
    }
}
