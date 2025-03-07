public class Main {

    public static void main(String[] args) {
        // String imdbFilePath = "buffer-manager\\data\\title.basics.tsv"; //change path on your machine, MANUALLY
        // int bufferPoolSize = 3;

        // BufferManager bufferManager = new LRUBufferManager(bufferPoolSize);
        // Utilities.loadDataset(bufferManager, imdbFilePath);

        BufferManager bufferManager = new BufferManagerLRU(Constants.BUFFER_SIZE);
        Utilities.loadDataset(bufferManager, Constants.IMDB_FILE_PATH);
    }
}
