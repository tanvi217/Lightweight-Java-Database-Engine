import java.io.IOException;

public class BufferManagerTest {

    public static void main(String[] args) {
        int totalPages = 10;
        int bufferSize = 4;
        BufferManager bm = new BufferManagerLRU(bufferSize);
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
