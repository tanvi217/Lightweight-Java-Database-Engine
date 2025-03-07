public class Main {

    public static void main(String[] args) {
        String IMDB_FILE_PATH = "C:\\Users\\abhis\\Desktop\\Priyanka\\UMass\\Spring25\\645\\lab1\\cs645-labs\\buffer-manager\\data\\title.basics.tsv\\title.basics.tsv"; //change path on your machine, MANUALLY
        //int bufferPoolSize = 3;

        // BufferManager bufferManager = new LRUBufferManager(bufferPoolSize);
        // Utilities.loadDataset(bufferManager, imdbFilePath);

        BufferManager bufferManager = new BufferManagerLRU(Constants.BUFFER_SIZE);
        Utilities.loadDataset(bufferManager, IMDB_FILE_PATH);
    }
}
