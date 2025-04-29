import java.util.*;
// import java.io.File;

public class join1 implements Operator {
    private Operator leftChild;
    private Operator rightChild;
    private BufferManager bufferManager;
    private int currentPageIndex = 0;
    private int currentRid = 0;
    private int N;
    private int[] tempPgIds;
    private int relevantIds; //used to track if on last run of this loop, tempPgIds isn't the full N

    public join1(Operator leftChild, Operator rightChild, BufferManager bufferManager) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.bufferManager = bufferManager;
        N = 1; //size of block TBD for now just 1
        tempPgIds = new int[N];
        relevantIds = N;
    }

    @Override
    public void open() {
        leftChild.open(); //selection on start-range and end-range with title
        rightChild.open(); //selection and projection branch, so here the child would be projection which has the child selection.
    }

    @Override
    public Row next() {
        //first we obtain the block of pages we are going to loop over and that are going to stay in the bm
        //change how we do this later but this is good for now

        Row currRow = leftChild.next();
        if(currRow == null){
            return null; //stopping criterion, basically at this point we are done
        }
        //need to account for this row in the loop so we are going to have an if statement in the loop.
        //This is not the cleanest but I am just going to do it for now

        //outer for loop loops to create the desired number of pages for our block
        for(int i =0; i<N; i++){
            Page currPage = bufferManager.createPage(Constants.JOIN1_TEMP_FILE_NAME, Constants.IMDB_ROW_LENGTH);
            tempPgIds[i] = currPage.getId();
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
                    break;
                }
                currPage.insertRow(currRow);
            }
            if(currRow ==null){
                break;
            }
        }

        //now we have the pages we need to read from to do the block nested loop join
        //will continue this later, basically need to join one row at a time
        //RETURN whenever we hit a match, because we want to return tuple at a time.
        currRow = rightChild.next();

        return null;
    }

    @Override
    public void close() {
        child.close();
        for (int pageId : tempPageIds) {
            bufferManager.unpinPage(pageId, TEMP_FILE_NAME);
            bufferManager.markClean(pageId, TEMP_FILE_NAME);
        }
        tempPageIds.clear();
    }

    private void materialize() {
        Page currentPage = bufferManager.createPage(TEMP_FILE_NAME, PROJECTED_ROW_LENGTH);
        int currentPageId = currentPage.getId();
        tempPageIds.add(currentPageId);

        while (true) {
            Row row = child.next();
            if (row == null) break;

            byte[] movieId = row.getAttribute(0, 9);
            byte[] personId = row.getAttribute(9, 10);
            Row projectedRow = new Row(movieId, personId);

            int inserted = currentPage.insertRow(projectedRow);
            if (inserted == -1) {
                bufferManager.unpinPage(currentPageId, TEMP_FILE_NAME);
                currentPage = bufferManager.createPage(TEMP_FILE_NAME, PROJECTED_ROW_LENGTH);
                currentPageId = currentPage.getId();
                tempPageIds.add(currentPageId);
                currentPage.insertRow(projectedRow);
            }

            bufferManager.markDirty(currentPageId, TEMP_FILE_NAME);
        }

        bufferManager.unpinPage(currentPageId, TEMP_FILE_NAME);
        materialized = true;
    }
}
