import java.util.ArrayList;
import java.util.List;

public class ProjectionOperator implements Operator {
    private Operator child;
    private List<Row> materializedRows;
    private int currentIndex;
    private boolean materialized;

    public ProjectionOperator(Operator child) {
        this.child = child;
        this.materializedRows = new ArrayList<>();
        this.currentIndex = 0;
        this.materialized = false;
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
        if (currentIndex >= materializedRows.size()) {
            return null;
        }
        return materializedRows.get(currentIndex++);
    }

    @Override
    public void close() {
        child.close();
        materializedRows.clear();
    }

    private void materialize() {
        while (true) {
            Row row = child.next();
            if (row == null) {
                break;
            }
            // movieId starts at 0, length 9
            byte[] movieId = row.getAttribute(0, 9);
            // personId starts at 9, length 10
            byte[] personId = row.getAttribute(9, 10);

            Row projectedRow = new Row(movieId, personId); // Create new Row with just movieId and personId
            materializedRows.add(projectedRow);
        }
        materialized = true;
    }
}
