import java.util.Iterator;
public class BTreeTest {
    public static void main(String[] args) {
        BTree<Integer> btree = new BufferBTree<Integer>(new LRUBufferManager(8), 2);    // Create a B-tree with max keys = 3
        
         //inserting keys into the B-tree
        System.out.println("inserting keys");
        insertAndPrint(btree, 10);
        insertAndPrint(btree, 20);
        insertAndPrint(btree, 30);
        insertAndPrint(btree, 40);
        insertAndPrint(btree, 50);
        
        //searching for a specific key
        System.out.println("Searching for key 30...");
        Iterator<Rid> searchResult = btree.search(30);
        if (searchResult.hasNext()) {
            System.out.println("key 30 found");
        } else {
            System.out.println("key 30 NOT found");
        }
        //searching for a specific key
        System.out.println("Searching for key 40...");
        Iterator<Rid> searchResult2 = btree.search(40);
        System.out.println(searchResult2.next());
        System.out.println(searchResult2.hasNext());
    }

    private static void insertAndPrint(BTree<Integer> btree, int key) {
        Rid rid = new Rid(key, key);    //creating Rid with key as both pageId and slotId
        btree.insert(key, rid); //insert key into the B-tree
        System.out.println("Inserted " + key);
    }
}
