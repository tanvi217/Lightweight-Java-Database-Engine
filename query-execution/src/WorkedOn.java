// WorkedOn (movieId: char(9), personId: char(10), category: char(20))
public class WorkedOn extends Relation {

    public static int[] movieId = {0, 9};
    public static int[] personId = {9, 19};
    public static int[] category = {19, 39};

    public WorkedOn(BufferManager bm, boolean randomTitle) {
        super("WorkedOn", new int[][] {movieId, personId, category}, bm, randomTitle);
    }

    public WorkedOn(BufferManager bm) { this(bm, false); }

}
