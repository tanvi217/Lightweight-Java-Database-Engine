import java.util.*;
import java.io.File;

public class ProjectionOperator implements Operator {
    private final Operator child;
    private final BufferManager bufferManager;
    private final String TEMP_FILE_NAME = "__temp__";
    private final int PROJECTED_ROW_LENGTH = 19; // movieId(9) + personId(10)

    private final List<Integer> tempPageIds = new ArrayList<>();
    private int currentPageIndex = 0;
    private int currentRid = 0;
    private boolean materialized = false;

    public ProjectionOperator(Operator child, BufferManager bufferManager) {
        this.child = child;
        this.bufferManager = bufferManager;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Row next() {
        if (!materialized) {
            materialize();
        }

        while (currentPageIndex < tempPageIds.size()) {
            Page page = bufferManager.getPage(tempPageIds.get(currentPageIndex), TEMP_FILE_NAME);
            if (currentRid < page.height()) {
                return page.getRow(currentRid++);
            } else {
                bufferManager.unpinPage(tempPageIds.get(currentPageIndex), TEMP_FILE_NAME);
                currentPageIndex++;
                currentRid = 0;
            }
        }

        return null;
    }

    @Override
    public void close() {
        child.close();
        for (int pageId : tempPageIds) {
            bufferManager.unpinPage(pageId, TEMP_FILE_NAME);
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
