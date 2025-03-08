import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;

public class Utilities {

    // Loads the buffer manager with the imdb dataset
    // Interleaves row insertion with createPage and getPage methods
    public static void loadDataset(BufferManager bf, String filepath) {
        try (BufferedReader bufferReader = new BufferedReader(new FileReader(filepath))) {
            String curRow;
            int rowsProcessed = 1;
            int possibleBufferPoolSize = Constants.BUFFER_SIZE;
            int rowsToFill = 5;
            int pagesCreated = 0;
            Random rand = new Random();

            bufferReader.readLine(); // Skip title row

            // create enough pages to ensure subsequent eviction
            for (int i = 0; i < possibleBufferPoolSize; i++) {
                Page currentPage = bf.createPage();
                pagesCreated++;
                int append_pid = currentPage.getId();
                int rowId = 0;

                System.out.println("Created page with Id " + append_pid);

                while ((curRow = bufferReader.readLine()) != null && !currentPage.isFull() && rowId < rowsToFill) {
                    String[] columns = curRow.split("\t");
                    byte[] movieId = columns[0].getBytes();
                    byte[] title = columns[2].getBytes();
                    Row row = new Row(movieId, title);

                    rowId = currentPage.insertRow(row);

                    // if row was inserted, mark the page dirty
                    if (rowId != -1) {
                        rowsProcessed++;
                        bf.markDirty(append_pid);

                        if (rowsProcessed % 1000 == 0) {
                            System.out.println("Processed " + rowsProcessed + " rows.");
                        }
                    }
                }

                bf.unpinPage(append_pid);
                System.out.println("Unpinned page with Id " + append_pid);
            }

            // getPage calls to ensure eviction
            // fill up pages pulled from buffer pool
            for (int i = 0; i < possibleBufferPoolSize; i++) {
                int randomPageId = rand.nextInt(pagesCreated);
                Page pageFromBufferPool = bf.getPage(randomPageId);

                if (pageFromBufferPool != null) {
                    System.out.println("Queried page with Id " + pageFromBufferPool.getId());
                }

                while ((curRow = bufferReader.readLine()) != null && !pageFromBufferPool.isFull()) {
                    String[] columns = curRow.split("\t");
                    byte[] movieId = columns[0].getBytes();
                    byte[] title = columns[2].getBytes();
                    Row row = new Row(movieId, title);

                    int rowId = pageFromBufferPool.insertRow(row);

                    // if row was inserted, mark the page dirty
                    if (rowId != -1) {
                        rowsProcessed++;
                        bf.markDirty(pageFromBufferPool.getId());

                        if (rowsProcessed % 1000 == 0) {
                            System.out.println("Processed " + rowsProcessed + " rows.");
                        }
                    }
                }

                bf.unpinPage(pageFromBufferPool.getId());

                System.out.println("Unpinned page with Id " + pageFromBufferPool.getId());
            }

            Page currentPage = bf.createPage();
            pagesCreated++;
            int append_pid = currentPage.getId();
            System.out.println("Created page with Id " + append_pid);

            // load rest of the dataset
            while ((curRow = bufferReader.readLine()) != null) {
                if (currentPage.isFull()) {
                    bf.unpinPage(currentPage.getId());
                    System.out.println("Unpinned page with Id " + currentPage.getId());

                    currentPage = bf.createPage();
                    pagesCreated++;
                    append_pid = currentPage.getId();
                    System.out.println("Created page with Id " + append_pid);
                }

                String[] columns = curRow.split("\t");
                byte[] movieId = columns[0].getBytes();
                byte[] title = columns[2].getBytes();
                Row row = new Row(movieId, title);
                int rowId = currentPage.insertRow(row);

                // if row was inserted, mark the page dirty
                if (rowId != -1) {
                    rowsProcessed++;
                    bf.markDirty(append_pid);

                    if (rowsProcessed % 1000 == 0) {
                        System.out.println("Processed " + rowsProcessed + " rows.");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}