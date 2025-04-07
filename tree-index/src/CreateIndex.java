import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;

public class CreateIndex {

    public static void main(String[] args) {
        BufferManager bufferManager = new LRUBufferManager(250);

        int totalRowsInTable = loadDatasetSequentially(bufferManager, Constants.IMDB_TSV_FILE);

        // TestC1 - create index on title
        MRTempFile<String> titleIndexBTree = IndexTests.CreateIndex(bufferManager, Constants.TITLE_INDEX, 0);

        // TestC3 - Verify index on title
        IndexTests.verifyIndex(titleIndexBTree, bufferManager, "Conjuring", Constants.TITLE_INDEX);

        // TestC4 - Verify index on title
        IndexTests.verifyRange(titleIndexBTree, bufferManager, "Boat Race", "Conjuring", Constants.TITLE_INDEX);

        // TestC2 - create index on movieId
        MRTempFile<String> movieIdIndexBTree = IndexTests.CreateIndex(bufferManager, Constants.MOVIE_ID_INDEX, 0);

        // TestC3 - Verify index on movieId
        IndexTests.verifyIndex(movieIdIndexBTree, bufferManager, "tt0000137", Constants.MOVIE_ID_INDEX);

        // TestC4 - Verify index on movieId
        IndexTests.verifyRange(titleIndexBTree, bufferManager, "tt0000137", "tt0000147", Constants.MOVIE_ID_INDEX);

        // TestP1 - Range query comparision on title
        String[][] rangesMovieTitle = {
            {"A Hard Wash", "Awakening of Rip"},
            {"A Hard Wash", "Conjuring"},
            {"A Hard Wash", "Grandes manoeuvres"}
        };

        IndexTests.compareRangeSearch(titleIndexBTree, bufferManager, rangesMovieTitle, 1, totalRowsInTable);

        // TestP2 - Range query comparision on Id
        String[][] rangesIds = {
            {"tt0000001", "tt0000013"},
            {"tt0000001", "tt0000046"},
            {"tt0000001", "tt0000078"}
        };

        IndexTests.compareRangeSearch(movieIdIndexBTree, bufferManager, rangesIds, 0, totalRowsInTable);

        // TestP3 - Keeping initial pages pinned
        MRTempFile<String> titleIndexBTreePinned = IndexTests.CreateIndex(bufferManager, Constants.TITLE_INDEX, 10);
        MRTempFile<String> movieIdIndexBTreePinned = IndexTests.CreateIndex(bufferManager, Constants.MOVIE_ID_INDEX, 10);

        IndexTests.compareRangeSearch(titleIndexBTreePinned, bufferManager, rangesMovieTitle, 1, totalRowsInTable);

        IndexTests.compareRangeSearch(movieIdIndexBTreePinned, bufferManager, rangesIds, 0, totalRowsInTable);
    }

    public static int loadDatasetSequentially(BufferManager bf, String csvFile) {
        int rowsProcessed = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String curRow;
            int pageId = 0;
            Page currentPage = bf.createPage();
            curRow = br.readLine(); // Skip title row
            int pagesCreated = 0;
            boolean skipLongMovieId = true;
            int skippedMovies = 0;

            while (curRow != null) {

                if (rowsProcessed == 20000) {
                    break;
                }
                
                if (currentPage.isFull()) {
                    bf.unpinPage(currentPage.getId());

                    currentPage = bf.createPage();
                    pagesCreated++;
                    pageId = currentPage.getId();
                }

                String[] columns = curRow.split("\t");
                byte[] movieId = columns[0].getBytes();

                if (skipLongMovieId && movieId.length > Constants.MOVIE_ID_SIZE) {
                    skippedMovies++;
                    curRow = br.readLine();
                    continue;
                }

                byte[] title = columns[2].getBytes();
                Row row = new Row(movieId, title);
                int rowId = currentPage.insertRow(row);

                // if row was inserted, mark the page dirty
                if (rowId != -1) {
                    rowsProcessed++;
                    bf.markDirty(pageId);

                    if (rowsProcessed % 1000000 == 0) {
                        System.out.println("Processed row with ID " + rowId + " in page with Id " + currentPage.getId());
                        System.out.println(rowsProcessed + " rows processed.");
                    }

                    curRow = br.readLine();
                }
            }

            bf.unpinPage(currentPage.getId());
            System.out.println(skippedMovies + " rows skipped due to long movieId.");
            System.out.println("Processed " + rowsProcessed + " rows, Created " + pagesCreated + " pages.");
            System.out.println("Finished loading dataset.");

            bf.force();
            System.out.println("Flushed to disk any remaining pages.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rowsProcessed;
    }

    // Loads the buffer manager with the imdb dataset
    // Interleaves row insertion with createPage and getPage methods
    public static void loadInterleavedDataset(BufferManager bf, String filepath) {
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