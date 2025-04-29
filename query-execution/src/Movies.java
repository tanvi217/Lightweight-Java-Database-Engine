// Movies (movieId: char(9), title: char(30))
public class Movies extends Schema {

    public static int[] movieId = {0, 9};
    public static int[] title = {9, 39};

    public Movies(BufferManager bm) {
        super("Movies", new int[][] {movieId, title}, bm);
    }

}
