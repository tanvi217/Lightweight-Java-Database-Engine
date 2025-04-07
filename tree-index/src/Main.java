import java.util.Iterator;
public class Main {

    public static void main(String[] args) {
        BufferManager bm = new LRUBufferManager();
        MRTempFile<Integer> tree = new MRTempFile<>(bm, 4);
        for (int i = 0; i < 20000; ++i) {
            tree.insert(i, new Rid(1, 2));
        }
        System.out.println(tree);
        Iterator<Rid> itr = tree.search(2300);
        while (itr.hasNext()) {
            System.out.println(itr.next());
        }
    }

}