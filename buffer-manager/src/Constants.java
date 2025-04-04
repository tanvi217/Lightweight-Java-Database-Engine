public class Constants {
    public static final int PAGE_KB = 4;
    public static final int BUFFER_SIZE = 8; // Default buffer size
    public static final int IMDB_ROW_LENGTH = 39; // Row size in bytes
    public static final int MOVIE_ID_INDEX = 0;
    public static final int MOVIE_ID_SIZE = 9; // movieId size: 9
    public static final int TITLE_INDEX = 1;
    public static final int TITLE_SIZE = 30; // title size: 30
    public static final int IMDB_MOVIE_ID_SIZE = 9; // movieId size: 9
    public static final int IMDB_TITLE_SIZE = 30; // title size: 30
    public static final String DATA_DIRECTORY = "../data/";
    public static final String IMDB_TSV_FILE = DATA_DIRECTORY + "title.basics.tsv";
    public static final String BINARY_DATA_FILE = "data.bin";
    public static final String INDEX_DATA_FILE = "index.bin";
    public static final int MAX_PAGE_ROWS = PAGE_KB * 1024 / IMDB_ROW_LENGTH;
}
