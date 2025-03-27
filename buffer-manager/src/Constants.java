public class Constants {
    public static final int PAGE_SIZE = 4096; // Page size in bytes
    public static final int ROW_SIZE = 39; // Row size in bytes
    public static final int MOVIE_ID_SIZE = 9; // movieId size: 9
    public static final int TITLE_SIZE = 30; // title size: 30
    public static final int MAX_ROWS = (PAGE_SIZE - 4) / ROW_SIZE; // Maximum number of rows per page, 4 bytes for records count (per packed page format)
    public static final int BUFFER_SIZE = 3; // Default buffer size
    public static final String BINARY_FILE_PATH = "../data/imdb_db.bin"; // Path to the binary file
    public static final String IMDB_FILE_PATH = "../data/title.basics.tsv"; // Relative path to the IMDB file
    public static final String IMDB_TSV_FILE = "../data/title.basics.tsv";
    public static final String DATA_BIN_FILE = "../data/imdb.bin";
}  