import java.nio.charset.StandardCharsets;

public class selectionMovie implements Operator{
    private String start_range;
    private String end_range;
    private Operator child;
    public selectionMovie(Operator child, String start_range, String end_range){
        this.start_range = start_range;
        this.end_range = end_range;
        this.child = child;
    }
    
    @Override
    public void open() {
        //nothing really necessary to do except open child
        System.out.println("Opened selectionMovie");
        child.open();
    }

    @Override
    public Row next() {
        Row r = child.next();
        if (r == null) {
            return null;
        }
        //extracts title (hopefully) and converts to a string
        String currTitle = new String(r.getAttribute(Constants.IMDB_MOVIE_ID_SIZE, Constants.IMDB_TITLE_SIZE), StandardCharsets.UTF_8);
        if(currTitle.compareTo(start_range)>=0 && currTitle.compareTo(end_range)<=0){
            return r;
        }
        return null;
    } 

    @Override
    public void close() {
        child.close();
    }
}