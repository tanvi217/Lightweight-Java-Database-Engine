import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LoadIMDb {

    public static void main(String[] args) {
        int bufferSize = 50;
        int rows = 10000;

        BufferManager bm = new LRUBufferManager(bufferSize);
        Relation movies = new Movies(bm, false);
        Relation workedOn = new WorkedOn(bm, false);
        Relation people = new People(bm, false);

        loadTableFromTSV(rows, "title.basics", movies, 0, 2);
        loadTableFromTSV(rows, "title.principals", workedOn, 0, 2, 3);
        loadTableFromTSV(rows, "name.basics", people, 0, 1);
    }

    private static void loadTableFromTSV(int numRows, String tsvTitle, Relation relation, int... attrIndices) {
        if (attrIndices.length != relation.attrRanges.length) {
            throw new IllegalStateException("Mismatch in number of attributes.");
        }
        String pathToTSV = Constants.DATA_DIRECTORY + tsvTitle + ".tsv";
        try (BufferedReader br = new BufferedReader(new FileReader(pathToTSV))) {
            br.readLine(); // skipping the header row
            int numInserted = 0;
            while (true) {
                String line = br.readLine();
                if (line == null || (numRows > 0 && numInserted >= numRows)) {
                    System.out.println("Loaded " + numInserted + " rows from " + tsvTitle + ".tsv into " + relation.tableTitle + ".bin");
                    break;
                }
                boolean skip = false;
                String[] values = line.split("\t");
                ByteBuffer rowData = ByteBuffer.allocate(relation.rowLength());
                for (int i = 0; i < attrIndices.length; ++i) {
                    byte[] attrBytes = values[attrIndices[i]].getBytes(StandardCharsets.UTF_8);
                    int[] range = relation.attrRanges[i];
                    if (attrBytes.length > range[1] - range[0]) {
                        attrBytes = Arrays.copyOf(attrBytes, range[1] - range[0]);
                        if (range[1] == 9 && relation.tableTitle.contains("Movies")) {
                            skip = true; // only skipping long movieIds
                            break; // out of inner loop only
                        }
                    }
                    rowData.position(range[0]);
                    rowData.put(attrBytes);
                }
                if (!skip) {
                    Row toInsert = new Row(rowData.clear());
                    relation.insertRow(toInsert, false);
                    ++numInserted;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        relation.saveToRelationsCSV();
    }

}
