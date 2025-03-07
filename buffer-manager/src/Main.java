
public class Main {

    public static void main(String[] args) {
        System.out.println("CS 645 Lab 1: Buffer Manager");
        
        /*
        testing Page.java ADDING ROW
        byte[] b1Arr = {0x01, 0x02, 0x03};
        byte[] b2Arr = {0x05, 0x06, 0x07};
        Row r1 = new Row(b1Arr, b2Arr);
        UnnamedPage p = new UnnamedPage();
        System.out.println(p.insertRow(r1));
        */

        
        //String imdbFilePath = "D:\\masters\\cs645-labs\\buffer-manager\\data\\title.basics.tsv";
        String imdbFilePath = "buffer-manager\\data\\title.basics.tsv"; //change path on your machine, MANUALLY
        int bufferPoolSize = 3;

        BufferManager bufferManager = new LRUBufferManager(bufferPoolSize);
        Utilities.loadDataset(bufferManager, imdbFilePath);
    }
}
