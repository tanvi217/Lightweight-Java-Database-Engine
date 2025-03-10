public class Main {

    public static void main(String[] args) {
        //experimented with different buffer sizes by changing Constants.BUFFER_SIZE
        BufferManager bufferManager = new LRUBufferManager(Constants.BUFFER_SIZE);
        Utilities.loadDataset(bufferManager, Constants.IMDB_FILE_PATH);
    }

}
