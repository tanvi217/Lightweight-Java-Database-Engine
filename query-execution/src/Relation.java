import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Relation {

    public final String tableTitle;
    public final int[][] attrRanges;
    public final int bytesInRow;
    public int numIOs = 0;

    private BufferManager bm;
    private static String pathToRelationsCSV = Constants.DATA_DIRECTORY + "relations.csv";
    private static Random rand = new Random(2L);

    public static String randomizeTitle(String tableTitle) {
        return tableTitle + "-" + rand.nextInt((int) 1e9);
    }

    /**
     * This class is intended to simplify the process of using a buffer manager.
     * Instead of passing the table's title every time you use the buffer manager,
     * you can keep Relation objects for each table and call buffer manager
     * functions using the object instead.
     * 
     * @param tableTitle    Associated with the file "{tableTitle}.bin" in the data
     *                      directory.
     * @param attrRanges    An array of integer pairs, defining the location of
     *                      attributes in this relation. The first integer is the
     *                      index of the first byte of the attribute in a Row of
     *                      this table. The second integer is the next index after
     *                      the last byte of the attribute. So the final element of
     *                      attrRanges is a pair where the second integer is the
     *                      length of the full row.
     * @param bufferManager The buffer manager that this object will make calls to.
     * @param randomTitle   If true, the given title will be given a random suffix.
     */
    public Relation(String tableTitle, int[][] attrRanges, BufferManager bufferManager, boolean randomTitle) {
        this.tableTitle = randomTitle ? randomizeTitle(tableTitle) : tableTitle;
        this.attrRanges = attrRanges;
        bytesInRow = attrRanges[attrRanges.length - 1][1];
        bm = bufferManager;
    }

    public Relation(String tableTitle, int[][] attrRanges, BufferManager bm) {
        this(tableTitle, attrRanges, bm, false);
    }

    public Relation(String tableTitle, int rowLength, BufferManager bm, boolean randomTitle) {
        this(tableTitle, new int[][] {new int[] {0, rowLength}}, bm, randomTitle);
    }

    public Relation(String tableTitle, int rowLength, BufferManager bm) {
        this(tableTitle, rowLength, bm, false);
    }

    public int getPageCount() {
        return bm.getPageCount(tableTitle);
    }

    public Page createPage() {
        numIOs +=1;
        return bm.createPage(tableTitle, bytesInRow);
    }

    public Page getPage(int pid) {
        numIOs+=1;
        return bm.getPage(pid, tableTitle);
    }

    public void unpinPage(int pid) {
        bm.unpinPage(pid, tableTitle);
    }

    public void markDirty(int pid) {
        bm.markDirty(pid, tableTitle);
    }

    public void markClean(int pid) {
        bm.markClean(pid, tableTitle);
    }

    /**
     * Inserts row, and returns the pageId of the page that will be inserted into on the next call of insertRow(), assuming that the pages are otherwise unchanged.
     * This means we can check if insertRow() filled the last slot in a page, as the pageId returned will be different from the pageId inserted into.
     */
    public int insertRow(Row nextRow, boolean clean) {
        int lastPid = getPageCount() - 1;
        Page target;
        if (lastPid < 0) {
            target = createPage();
            lastPid = target.getId();
        } else {
            Page last = getPage(lastPid);
            if (last.isFull()) {
                unpinPage(lastPid);
                target = createPage();
                lastPid = target.getId();
            } else {
                target = last;
            }
        }
        target.insertRow(nextRow);
        boolean filled = target.isFull();
        if (clean) {
            markClean(lastPid);
        } else {
            markDirty(lastPid); 
        }
        unpinPage(lastPid); // lastPid is same as targetPid at this point
        return filled ? lastPid + 1 : lastPid;
    }

    public int rowLength() {
        return bytesInRow;
    }

    public void delete() {
        bm.setPageCount(0, tableTitle);
        File binFile = new File(Constants.DATA_DIRECTORY + tableTitle + ".bin");
        if (binFile.delete()) {
            System.out.println(tableTitle + " deleted successfully.");
        } else {
            System.out.println(tableTitle + " was not deleted.");
        }
    }

    public static String rowToString(Row row, int... rangeEnds) {
        if (row == null) {
            return "null row";
        }
        StringBuilder sb = new StringBuilder();
        int rangeStart = 0;
        for (int end : rangeEnds) {
            sb.append(row.getString(rangeStart, end));
            sb.append(',');
            rangeStart = end;
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    /**
     * Returns a string formatted to be used in the data directory's relations.csv
     * file. The tableTitle, number of pages accounted for by the buffer manager,
     * and attribute range end indices are included.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(tableTitle);
        sb.append(',');
        sb.append(bm.getPageCount(tableTitle));
        for (int[] range : attrRanges) {
            sb.append(',');
            sb.append(range[1]);
        }
        return sb.toString();
    }

    /**
     * Adds a row to relations.csv, to record the number of pages in the associated
     * binary file. The added row is generated by this object's toString function.
     */
    public void saveToRelationsCSV() {
        bm.force();
        List<String> lines = new ArrayList<>();
        boolean addLine = true;
        try (BufferedReader br = new BufferedReader(new FileReader(pathToRelationsCSV))) {
            // don't skip the header row
            while (true) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                String[] values = line.split(",");
                if (values[0].equals(tableTitle)) {
                    lines.add(toString());
                    addLine = false;
                } else {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (addLine) {
            lines.add(toString());
        }
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(pathToRelationsCSV))) {
            for (String line : lines) {
                bw.write(line);
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Searches relations.csv for the given tableTitle. If there is a matching row
     * in the CSV, then we read it to create a Relation object. The first number in
     * the row after the fileTitle is the number of pages in disk. The remaining
     * numbers are the ends of the attribute ranges. The buffer manager is updated
     * with the number of pages recorded in the CSV file, so make sure that a
     * binary file of the correct size actually exists in the data directory.
     * 
     * @param tableTitle    The name of the table. Must be the first value of some
     *                      row in the relations.csv file.
     * @param bufferManager The buffer manager to pass to the new object.
     */
    public static Relation retrieveFromRelationsCSV(String tableTitle, BufferManager bufferManager) {
        int pageCount = 0;
        int[][] ranges = null;
        try (BufferedReader br = new BufferedReader(new FileReader(pathToRelationsCSV))) {
            br.readLine(); // skipping the header row
            while (true) {
                String line = br.readLine();
                if (line == null) {
                    return null;
                    // Table with this title was not found in relations.csv
                }
                String[] values = line.split(",");
                if (values[0].equals(tableTitle)) {
                    pageCount = Integer.parseInt(values[1]);
                    ranges = new int[values.length - 2][2];
                    for (int i = 0; i < ranges.length; ++i) {
                        ranges[i][0] = (i > 0) ? ranges[i - 1][1] : 0; // if this is the first range the start index is 0, otherwise it is the end index of the previous range
                        ranges[i][1] = Integer.parseInt(values[i + 2]);
                    }
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        bufferManager.setPageCount(pageCount, tableTitle);
        return new Relation(tableTitle, ranges, bufferManager);
    }

}
