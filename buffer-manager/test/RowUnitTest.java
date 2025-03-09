import java.util.Arrays;

public class RowUnitTest {
    public static void main(String[] args){
        // Test case 1: Padding with short title
        byte[] title1 = "Test".getBytes(); // Title is shorter than the required size
        Row row = new Row(new byte[Constants.MOVIE_ID_SIZE], title1);

        if (row.title.length == Constants.TITLE_SIZE) {
            System.out.println("Test 1 passed");
        } else {
            System.out.println("Test 1 failed");
        }
        //Test 2: Testing serialize/deserialize method and padding
        byte[] movieId = "123".getBytes();
        byte[] title2 = "La La Land".getBytes();
        
        Row row2 = new Row(movieId, title2);

        byte[] serializedRow = row2.serialize();

        Row deserializedRow = Row.deserialize(serializedRow);

        // Check if movieId and title match after deserialization and padding
        boolean movieIdMatches = Arrays.equals(Arrays.copyOf(deserializedRow.movieId, movieId.length), movieId);
        boolean titleMatches = new String(deserializedRow.title).trim().equals("La La Land");

        if (movieIdMatches && titleMatches) {
            System.out.println("Test 2 passed");
        } else {
            System.out.println("Test 2 failed");
        }

    }
}
