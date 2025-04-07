public class Main {

    public static void main(String[] args) {
        BufferManager bm = new LRUBufferManager();
        MRTempFile<Integer> tree = new MRTempFile<>(bm, 4);
        for (int i = 0; i < 200000; ++i) {
            tree.insert(i, new Rid(1, 2));
        }
        System.out.println(tree);
    }

}