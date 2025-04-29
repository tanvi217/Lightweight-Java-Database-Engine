// People (personId: char(10), name: char(105))
public class People extends Schema {

    public static int[] personId = {0, 10};
    public static int[] name = {10, 115};

    public People(BufferManager bm) {
        super("People", new int[][] {personId, name}, bm);
    }

}
