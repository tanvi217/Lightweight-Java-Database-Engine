// People (personId: char(10), name: char(105))
public class People extends Relation {

    public static int[] personId = {0, 10};
    public static int[] name = {10, 115};

    public People(BufferManager bm, boolean randomTitle) {
        super("People", new int[][] {personId, name}, bm, randomTitle);
    }

    public People(BufferManager bm) { this(bm, false); }

}
