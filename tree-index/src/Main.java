import java.util.Iterator;
public class Main {

    public static void main(String[] args) {
        BufferManager bm = new LRUBufferManager();
        BTree<Integer> tree = new MRTempFile<Integer>(bm, 4);
        for (int i = 0; i < 20000; ++i) {
            tree.insert(i, new Rid(1, i));
        }
        System.out.println(tree);
        Iterator<Rid> itr = tree.rangeSearch(18000, 18002);
        int j = 0;
        while (itr.hasNext()) {
            Rid next = itr.next();
            if (j < 10) {
                System.out.println(next);
            }
            ++j;
        }
        System.out.println("Number of matches: " + j);
    }

}