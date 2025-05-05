// Movies (movieId: char(9), title: char(30))
public class Movies extends Relation {

    public static int[] movieId = {0, 9};
    public static int[] title = {9, 39};

    public Movies(BufferManager bm, boolean randomTitle) {
        super("Movies", new int[][] {movieId, title}, bm, randomTitle);
    }

    public Movies(BufferManager bm) { this(bm, false); }

    public static void main(String[] args) {
        for (String s : args) {
            System.out.println(s);
        }
    }

}
