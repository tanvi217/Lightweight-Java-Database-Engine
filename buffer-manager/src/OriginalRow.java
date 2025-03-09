/**
 * Struct representing a database row, *containing primitive data types ONLY* to
 * enable serialization.
 */
public class OriginalRow extends Row {

    // Define primary data type fields, depending on the schema of the table
    // These fields are for the Movies table described below
    // public byte[] movieId;
    // public byte[] title;

    public OriginalRow(byte[] movieId, byte[] title) { // altered only to extend Row
        super.movieId = movieId;
        super.title = title;
    }

}
