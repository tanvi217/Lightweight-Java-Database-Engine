public class RunIMDbQuery {

    private static boolean useMovieIndex = false;

    public static void main(String[] args) {
        int bufferSize = args.length < 3 ? 200 : Integer.parseInt(args[2]); // read from input

        BufferManager bm = new LRUBufferManager(bufferSize);
        Relation movies = Relation.retrieveFromRelationsCSV("Movies", bm);
        Relation workedOn = Relation.retrieveFromRelationsCSV("WorkedOn", bm);
        Relation people = Relation.retrieveFromRelationsCSV("People", bm);

        String startRange = args.length < 1 ? "AB" : args[0]; // read from input
        String endRange = args.length < 2 ? "CD" : args[1]; // read from input

        // left side of first BNL join

        Operator readMovies;
        if (useMovieIndex) {
            boolean useSavedBTree = false;
            readMovies = new IndexOperator(movies, Movies.title, startRange, bm, useSavedBTree);
        } else {
            readMovies = new ScanOperator(movies, true);
        }
        Operator selectMovies = new SelectionOperator(readMovies, Movies.title, startRange, endRange);
        
        // right side of first BNL join
        Operator readWorkedOn = new ScanOperator(workedOn, false);
        Operator selectWorkedOn = new SelectionOperator(readWorkedOn, WorkedOn.category, "director");
        Operator projWorkedOn = new ProjectionOperator(selectWorkedOn, bm, WorkedOn.movieId, WorkedOn.personId);

        // left side of second BNL join
        int[] projMovieId = WorkedOn.movieId; // the movieId range was unchanged by the projection
        int firstBlockSize = (bufferSize - 10) / 2; // TODO calculate
        Operator firstJoin = new BNLJoinOperator(
            selectMovies, projWorkedOn,
            Movies.movieId, projMovieId,
            bm, firstBlockSize
        ); // firstJoin schema: movieId, title, personId

        // right side of second BNL join
        Operator readPeople = new ScanOperator(people, true);
        
        // final result
        int[] joinedPersonId = new int[] {Movies.title[1], Movies.title[1] + People.personId[1]}; // Movies.title[1] is full length of movieId + title pair
        int secondBlockSize = firstBlockSize;
        Operator secondJoin = new BNLJoinOperator(
            firstJoin, readPeople,
            joinedPersonId, People.personId,
            bm, secondBlockSize
        ); // secondJoin schema: personId, movieId, title, name

        int[] joinedTitle = new int[] {People.personId[1] + Movies.movieId[1], People.personId[1] + Movies.title[1]};
        int[] joinedName = new int[] {Movies.title[1] + People.personId[1], Movies.title[1] + People.name[1]};
        Operator projJoined = new ProjectionOperator(secondJoin, joinedTitle, joinedName);

        int titleLength = Movies.title[1] - Movies.title[0];
        int nameLength = People.name[1] - People.name[0];

        Operator test = projJoined;
        test.open();
        for (int i = 0; i < 1000; ++i) {
            Row next = test.next();
            System.out.println((i + 1) + " " + Relation.rowToString(next, titleLength, titleLength + nameLength));
            if (next == null) {
                break;
            }
        }
        test.close();

    }

}
