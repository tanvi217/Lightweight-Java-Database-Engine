import java.util.*;
// import java.io.File;

public class join1 implements Operator {
    private Operator leftChild;
    private Operator rightChild;
    private BufferManager bufferManager;
    private int N;
    private int[] tempPgIds;
    private int relevantIds; //used to track if on last run of this loop, tempPgIds isn't the full N
    private final Map<String, List<Rid>> hashTable;
    private boolean needToCreateNewBlock;
    private Row currRightRow = null;
    private int matchIndex;
    private List<Rid> currentMatchList = new ArrayList<>();


    public join1(Operator leftChild, Operator rightChild, BufferManager bufferManager) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.bufferManager = bufferManager;
        N = 1; //size of block TBD for now just 1
        tempPgIds = new int[N];
        relevantIds = N;
        needToCreateNewBlock = true;
        this.hashTable = new HashMap<>();
    }

    @Override
    public void open() {
        leftChild.open(); //selection on start-range and end-range with title
        rightChild.open(); //selection and projection branch, so here the child would be projection which has the child selection.
        //initializing values, probably not necessary but doesn't hurt to do it again
        needToCreateNewBlock = true;
        currRightRow = null;
        currentMatchList = new ArrayList<>();
        matchIndex = 0;
    }

    //if we return True that means we must return null
    private boolean createPageBlock(){
        hashTable.clear(); // Clear previous block's entries
        // potentially change how we do this later but this is good for now

        Row currRow = leftChild.next();
        if(currRow == null){
            return true; //stopping criterion, basically at this point we are done
        }
        //need to account for this row in the loop so we are going to have an if statement in the loop.
        //This is not the cleanest but I am just going to do it for now

        //outer for loop loops to create the desired number of pages for our block
        for(int i =0; i<N; i++){
            Page currPage = bufferManager.createPage(Constants.JOIN1_TEMP_FILE_NAME, Constants.IMDB_ROW_LENGTH);
            tempPgIds[i] = currPage.getId();
            //mark them clean so not ever writtent to disk
            bufferManager.markClean(tempPgIds[i], Constants.JOIN1_TEMP_FILE_NAME);
            if(i ==0){
                //inserting the first row because we are checking before loop to see if we need to return a null.
                currPage.insertRow(currRow);
            }
            //don't mark dirty because we don't want to persist to disk
            while(!currPage.isFull()){
                //inserts rows to our temptable block one at a time, if we hit -1 then that page is full
                currRow = leftChild.next();
                if(currRow == null){
                    //stop because no more rows to read
                    relevantIds = i; //setting relevantIds appropriately.
                    return false;
                }
                //inserts the row into the current page and adds the record id key value pair into the hash table
                
                int slotId = currPage.insertRow(currRow);
                String joinKey = new String(currRow.getAttribute(0, Constants.MOVIE_ID_SIZE));
                hashTable.computeIfAbsent(joinKey, k -> new ArrayList<>())
                    .add(new Rid(currPage.getId(), slotId));
            }
            if(currRow ==null){
                System.out.println("Should never get here");
                return false;
            }
        }
        //this line is a bit redundant because relevantIds should never go below N unless we have no more rows to add, so this line is never
        //reached when reelvantIds is not already equal to N.
        relevantIds = N;
        return false;
    }
    @Override
    public Row next() {
        //first we obtain the block of pages we are going to loop over and that are going to stay in the bm
        //we call the above function, and it returns TRUE if we ran out of pages right at the start and therefore need to return null
        //have a variable for when to create next block, starts at true when BNL is started and will change back to true whenever we finish
        //a block
        while(true){
            //create a new block if necessary and check to see if we are done
            if(needToCreateNewBlock){
                needToCreateNewBlock= false;
                boolean done = createPageBlock();
                if(done){
                    return null;
                }
                System.out.println("Made it past done");
                //initializing everything
                currRightRow = rightChild.next();
                matchIndex = 0;
                currentMatchList = new ArrayList<>();
            }
            //now the block is created so we will go through each right row
            while(currRightRow !=null){
                //need this check in case we call this next again
                if(currentMatchList.isEmpty()){
                    String rightKey = new String(currRightRow.getAttribute(0, Constants.WORKEDON_MOVIEID_SIZE));
                    currentMatchList = hashTable.getOrDefault(rightKey, Collections.emptyList());
                    matchIndex = 0;
                }
                //loop through the matchList
                while(matchIndex < currentMatchList.size()){
                    Rid rid = currentMatchList.get(matchIndex++);
                    Page page = bufferManager.getPage(rid.getPageId(), Constants.JOIN1_TEMP_FILE_NAME);
                    Row leftRow = page.getRow(rid.getSlotId());
                    return new Row(
                        leftRow.getAttribute(0, Constants.MOVIE_ID_SIZE),
                        leftRow.getAttribute(Constants.MOVIE_ID_SIZE, Constants.TITLE_SIZE),
                        currRightRow.getAttribute(Constants.WORKEDON_MOVIEID_SIZE, Constants.WORKEDON_PERSONID_SIZE)
                    );
                }
                //now we went through all the matches so we just need to move on to the next right row
                currRightRow = rightChild.next();
                currentMatchList = new ArrayList<>();
                matchIndex = 0;
            }

            //now we finished all the right rows and need to restart with a new block
            //we KNOW that rightChild.next() will now return the results starting back at the beginning
            //this was an implemnetation choice since it kind of "loops"
            //so we just need to address unpinning our temp pages
            for(int i = 0; i< relevantIds; i++){
                //we marked clean already, but this is doing it a second time just to be safe
                bufferManager.unpinPage(tempPgIds[i], Constants.JOIN1_TEMP_FILE_NAME);
                bufferManager.markClean(tempPgIds[i], Constants.JOIN1_TEMP_FILE_NAME);
            }
            needToCreateNewBlock=true;
        }
         
        /* 
        //now we have the pages we need to read from to do the block nested loop join, saved in tempPgIds
        //will continue this later, basically need to join one row at a time
        //RETURN whenever we hit a match, because we want to return tuple at a time.
        //since Ids are unique we can return as soon as we find one
        Row currRow = rightChild.next();
        //this loop is looping through our OUTER table, that is not what we want. We want to loop through the inner table
        while (currentPageIndex < relevantIds) {
            int currPageId = tempPgIds[currentPageIndex];
            Page page = bufferManager.getPage(currPageId, Constants.JOIN1_TEMP_FILE_NAME);
            //now we have the page and want to check the join condition
            if (currentRid < page.height()) {
                //gets the String version of Movie id from the outer/left table
                String leftMovieId = new String(page.getRow(currentRid).getAttribute(0, Constants.MOVIE_ID_SIZE));
                //gets the String version of the Movie id from the innner/right table WorkedOn
                String rightMovieId = new String(currRow.getAttribute(0, Constants.WORKEDON_MOVIEID_SIZE));
                if(leftMovieId.equals(rightMovieId)){
                    //make the new row of this "joined table"
                    // we will do movie id, then title, then personid then category size
                    Row r = new Row(
                        page.getRow(currentRid).getAttribute(0, Constants.MOVIE_ID_SIZE),
                        page.getRow(currentRid).getAttribute(Constants.MOVIE_ID_SIZE, Constants.TITLE_SIZE),
                        currRow.getAttribute(Constants.WORKEDON_MOVIEID_SIZE, Constants.WORKEDON_PERSONID_SIZE),
                        currRow.getAttribute(Constants.WORKEDON_PERSONID_SIZE, Constants.WORKEDON_CATEGORY_SIZE)
                    );

                    return r;
                }
                return page.getRow(currentRid++);
            } else {
                bufferManager.unpinPage(tempPageIds.get(currentPageIndex), TEMP_FILE_NAME);
                currentPageIndex++;
                currentRid = 0;
            }
        }
        //resetting currentPageIndex and currentRid back to 0. the reason for this is we will return null ONCE upon next call, and then
        //restart from the begginning.
        currentPageIndex = 0;
        currentRid =0;
        return null;
        */
        
    }

    @Override
    public void close() {
        leftChild.close();
        rightChild.close();
        //clear internal buffers, although probably not necessary
        hashTable.clear();
        currRightRow = null;
        currentMatchList.clear();
        //don't need to unpin pages because we do that as we go along
    }

}
