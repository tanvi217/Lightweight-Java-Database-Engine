import java.util.Iterator;
public class Main {

    public static void main(String[] args) {
        BufferManager bm = new LRUBufferManager();
        BTree<String> tree = new MRTempFile<String>(bm, 6, 0, true);
        for (int i = 0; i < 100000; ++i) {
            tree.insert(String.valueOf(i), new Rid(1, i));
        }
        System.out.println(tree);
        Iterator<Rid> itr = tree.rangeSearch("6", "604");
        int j = 0;
        while (itr.hasNext()) {
            Rid next = itr.next();
            if (j < 5) {
                System.out.println(next);
            }
            ++j;
        }
        System.out.println("Number of matches: " + j);
    }

}