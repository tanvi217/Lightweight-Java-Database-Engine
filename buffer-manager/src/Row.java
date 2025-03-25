import java.nio.charset.StandardCharsets;
public class Row {

    public byte[] movieId;
    public byte[] title;

    public Row(byte[] movieId, byte[] title) {
        this.movieId = movieId;
        this.title = title;
    }

    public String getMovieId(){
        return new String(this.movieId, StandardCharsets.UTF_8);
    }

    public String getTitle(){
        return new String(this.title, StandardCharsets.UTF_8);
    }

}