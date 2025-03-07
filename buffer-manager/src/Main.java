import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        //String IMDB_FILE_PATH = "C:\\Users\\abhis\\Desktop\\Priyanka\\UMass\\Spring25\\645\\lab1\\cs645-labs\\buffer-manager\\data\\title.basics.tsv\\title.basics.tsv"; //change path on your machine, MANUALLY
        //int bufferPoolSize = 3;

        // BufferManager bufferManager = new LRUBufferManager(bufferPoolSize);
        // Utilities.loadDataset(bufferManager, imdbFilePath);
        

        //BufferManager bufferManager = new BufferManagerLRU(Constants.BUFFER_SIZE);
        //Utilities.loadDataset(bufferManager, IMDB_FILE_PATH);
        bufferManagerLRUTests();
    }

    private static void bufferManagerLRUTests(){
        int totalPages = 10;
        int bufferSize = 4;
        BufferManager bm = new BufferManagerLRU(bufferSize, true);
        for (int i = 0; i < totalPages; ++i) {
            try {
                Page page = bm.createPage();
                System.out.println("A" + page.getId());
                bm.unpinPage(page.getId());
            } catch (IOException e) {
                System.out.println(e);
                break;
            }
            System.out.println("got page " + i + ": " + bm.toString());
        }
    }
}
