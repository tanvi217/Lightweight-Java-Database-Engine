public class RunIMDbQuery {

    public static void main(String[] args) {
        int bufferSize = 200; // read from input

        BufferManager bm = new LRUBufferManager(bufferSize);
        Relation movies = Relation.retrieveFromRelationsCSV("Movies", bm);
        Relation workedOn = Relation.retrieveFromRelationsCSV("WorkedOn", bm);
        Relation people = Relation.retrieveFromRelationsCSV("People", bm);

        String startRange = "AB"; // read from input
        String endRange = "TV"; // read from input

        // left side of first BNL join
        Operator readMovies = new IndexOperator(movies, Movies.title, startRange, bm);
        Operator selectMovies = new SelectionOperator(readMovies, Movies.title, startRange, endRange);
        
        // right side of first BNL join
        Operator readWorkedOn = new ScanOperator(workedOn, false);
        Operator selectWorkedOn = new SelectionOperator(readWorkedOn, WorkedOn.category, "director");
        Operator projWorkedOn = new ProjectionOperator(selectWorkedOn, bm, WorkedOn.movieId, WorkedOn.personId);

        // left side of second BNL join
        int[] projMovieId = WorkedOn.movieId; // the movieId range was unchanged by the projection
        int projRowLength = Movies.movieId[1] + People.personId[1]; // works since these ranges come first in their relations
        int firstBlockSize = 8; // TODO calculate
        Operator firstJoin = new BNLJoinOperator(
            selectMovies, projWorkedOn,
            Movies.movieId, projMovieId,
            movies.rowLength(), projRowLength,
            bm, firstBlockSize
        ); // firstJoin schema: movieId, title, personId

        //////
        Operator test = firstJoin;
        test.open();
        for (int i = 0; i < 5000; ++i) {
            Row next = test.next();
            if (i % 100 == 0) {
                System.out.println(next);
            }
        }
        test.close();
        if (true) { return; }
        //////

        // right side of second BNL join
        Operator readPeople = new ScanOperator(people, true);
        
        // final result
        int[] joinedPersonId = new int[] {Movies.title[1], Movies.title[1] + People.personId[1]}; // Movies.title[1] is full length of movieId + title pair
        int joinedRowLength = Movies.title[1] + People.personId[1];
        int secondBlockSize = 8; // TODO calculate
        Operator secondJoin = new BNLJoinOperator(
            firstJoin, readPeople,
            joinedPersonId, People.personId,
            joinedRowLength, people.rowLength(),
            bm, secondBlockSize
        ); // secondJoin schema: personId, movieId, title, name

        int[] joinedTitle = new int[] {People.personId[1] + Movies.movieId[1], People.personId[1] + Movies.title[1]};
        int[] joinedName = new int[] {WorkedOn.category[1] + Movies.title[1] - Movies.movieId[1], WorkedOn.category[1] + Movies.title[1] - Movies.movieId[1] + People.name[1] - People.personId[1]};
        Operator projJoined = new ProjectionOperator(secondJoin, joinedTitle, joinedName);
        
        projJoined.open();
        System.out.println(projJoined.next());
    }

}
