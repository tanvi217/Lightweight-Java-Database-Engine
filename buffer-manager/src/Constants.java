public class Constants {
    public static final int PAGE_KB = 4;
    public static final int BUFFER_SIZE = 8; // Default buffer size
    public static final int IMDB_ROW_LENGTH = 39; // Row size in bytes
    public static final int MOVIE_ID_INDEX = 0;
    public static final int MOVIE_ID_START_BYTE = 0;
    public static final int MOVIE_ID_SIZE = 9; // movieId size: 9
    public static final int TITLE_INDEX = 1;
    public static final int TITLE_START_BYTE = 9;
    public static final int TITLE_SIZE = 30; // title size: 30
    public static final int IMDB_MOVIE_ID_SIZE = 9; // movieId size: 9
    public static final int IMDB_TITLE_SIZE = 30; // title size: 30
    public static final int WORKEDON_MOVIEID_SIZE = 9; // movieId size: 9
    public static final int WORKEDON_PERSONID_SIZE = 10; // personId size: 10
    public static final int WORKEDON_CATEGORY_SIZE = 20; // category size: 20
    public static final int PERSON_ID_SIZE = 10; // personId size: 10
    public static final int PERSON_NAME_SIZE = 105; // personName size: 105
    public static final String DATA_DIRECTORY = "../data/";
    public static final String IMDB_TSV_FILE = DATA_DIRECTORY + "title.basics.tsv";
    public static final String IMDB_WORKED_ON_TSV_FILE = DATA_DIRECTORY + "title.principals.tsv";
    public static final String IMDB_PEOPLE_TSV_FILE = DATA_DIRECTORY + "name.basics.tsv";
    public static final String BINARY_DATA_FILE = "data.bin";
    public static final String INDEX_DATA_FILE = "index.bin";
    public static final int MAX_PAGE_ROWS = PAGE_KB * 1024 / IMDB_ROW_LENGTH;
    //new constants for lab 3
    public static final String TEMP_FILE_NAME = "__temp__"; //temp file produced from projection
    public static final int PROJECTED_ROW_LENGTH = 19; // size of rows from temp file ^, movieId(9) + personId(10)
    public static final String JOIN1_TEMP_FILE_NAME = "__join1temp__";
    public static final String JOIN2_TEMP_FILE_NAME = "__join2temp__";
    public static final int JOIN1_OUTPUT_ROW_LENGTH = 49; //9 for movie id, 30 for movie title, 10 for person id
}
