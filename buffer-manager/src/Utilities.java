import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;

public class Utilities {

    public static void main(String[] args) {
        //experimented with different buffer sizes by changing Constants.BUFFER_SIZE
        BufferManager bufferManager = new LRUBufferManager(Constants.BUFFER_SIZE);
        loadDataset(bufferManager, Constants.IMDB_FILE_PATH);
    }

    // Loads the buffer manager with the imdb dataset
    // Interleaves row insertion with createPage and getPage methods
    public static void loadDataset(BufferManager bf, String filepath) {
        try (BufferedReader bufferReader = new BufferedReader(new FileReader(filepath))) {
            String curRow;
            int rowsProcessed = 0;
            int possibleBufferPoolSize = Constants.BUFFER_SIZE + 5;
            int rowsToFill = 9;
            int pagesCreated = 0;
            Random rand = new Random();
            boolean skipLongMovieId = true;
            int skippedMovies = 0;

            curRow = bufferReader.readLine(); // Skip title row

            // create enough pages to ensure subsequent eviction
            for (int i = 0; i < possibleBufferPoolSize; i++) {
                Page currentPage = bf.createPage();
                pagesCreated++;
                int append_pid = currentPage.getId();
                int rowId = 0;
                curRow = bufferReader.readLine();

                System.out.println("Created page with Id " + append_pid);

                while (curRow != null && !currentPage.isFull() && rowId < rowsToFill) {
                    String[] columns = curRow.split("\t");
                    byte[] movieId = columns[0].getBytes();

                    if (skipLongMovieId && movieId.length > Constants.MOVIE_ID_SIZE) {
                        skippedMovies++;
                        curRow = bufferReader.readLine();
                        continue;
                    }

                    byte[] title = columns[2].getBytes();
                    Row row = new Row(movieId, title);

                    rowId = currentPage.insertRow(row);

                    // if row was inserted, mark the page dirty
                    if (rowId != -1) {
                        rowsProcessed++;
                        bf.markDirty(append_pid);

                        if (rowsProcessed % 10 == 0) {
                            System.out.println("Processed " + rowId + " rows in page with Id " + currentPage.getId());
                            System.out.println("Processed " + rowsProcessed + " rows.");
                        }

                        curRow = bufferReader.readLine();
                    } else {
                        System.err.println("rowsProcessed: " + rowsProcessed);
                        System.err.println(currentPage.getId() + " could not insert row");
                        break;
                    }
                }

                bf.unpinPage(append_pid);
            }

            // getPage calls to ensure eviction
            // fill up pages pulled from buffer pool
            for (int i = 0; i < possibleBufferPoolSize + 10; i++) {
                int randomPageId = rand.nextInt(pagesCreated);

                Page pageFromBufferPool = bf.getPage(randomPageId);

                if (pageFromBufferPool != null) {
                    System.out.println("Queried page with Id " + pageFromBufferPool.getId());
                }

                while (curRow != null && !pageFromBufferPool.isFull()) {
                    String[] columns = curRow.split("\t");
                    byte[] movieId = columns[0].getBytes();

                    if (skipLongMovieId && movieId.length > Constants.MOVIE_ID_SIZE) {
                        skippedMovies++;
                        curRow = bufferReader.readLine();
                        continue;
                    }

                    byte[] title = columns[2].getBytes();

                    Row row = new Row(movieId, title);

                    int rowId = pageFromBufferPool.insertRow(row);

                    // if row was inserted, mark the page dirty
                    if (rowId != -1) {
                        rowsProcessed++;
                        bf.markDirty(pageFromBufferPool.getId());

                        if (rowsProcessed % 100 == 0) {
                            System.out.println("Processed " + rowId + " rows in page with Id " + pageFromBufferPool.getId());
                            System.out.println(rowsProcessed + " rows processed.");
                        }

                        curRow = bufferReader.readLine();
                    } else {
                        System.err.println("rowsProcessed: " + rowsProcessed);
                        System.err.println(pageFromBufferPool.getId() + " could not insert row");
                        break;
                    }
                }

                bf.unpinPage(pageFromBufferPool.getId());
            }

            System.out.println("Finished querying. Processing rest of the dataset.");

            Page currentPage = bf.createPage();
            pagesCreated++;
            int append_pid = currentPage.getId();

            while (curRow != null) {
                
                if (currentPage.isFull()) {
                    bf.unpinPage(currentPage.getId());

                    currentPage = bf.createPage();
                    pagesCreated++;
                    append_pid = currentPage.getId();
                }

                String[] columns = curRow.split("\t");
                byte[] movieId = columns[0].getBytes();

                if (skipLongMovieId && movieId.length > Constants.MOVIE_ID_SIZE) {
                    skippedMovies++;
                    curRow = bufferReader.readLine();
                    continue;
                }

                byte[] title = columns[2].getBytes();
                Row row = new Row(movieId, title);
                int rowId = currentPage.insertRow(row);

                // if row was inserted, mark the page dirty
                if (rowId != -1) {
                    rowsProcessed++;
                    bf.markDirty(append_pid);

                    if (rowsProcessed % 1000000 == 0) {
                        System.out.println("Processed " + rowId + " rows in page with Id " + currentPage.getId());
                        System.out.println(rowsProcessed + " rows processed.");
                    }

                    curRow = bufferReader.readLine();
                }
            }

            bf.unpinPage(currentPage.getId());
            System.out.println(skippedMovies + " rows skipped due to long movieId.");
            System.out.println("Processed " + rowsProcessed + " rows, Created " + pagesCreated + " pages.");
            System.out.println("Finished loading dataset.");
            /*
            Page[] currentPages = (((LRUBufferManager) bf).getCurrPages());
            
            for (Page page : currentPages) {
                System.out.println(page.getId());
            }
            System.out.println(currentPages[0].insertRow(new Row("123456789".getBytes(), "TITLE".getBytes())));
            System.out.println(currentPages[1].insertRow(new Row("123456789".getBytes(), "TITLE".getBytes())));
            System.out.println(currentPages[2].insertRow(new Row("123456789".getBytes(), "TITLE".getBytes())));
            */
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}