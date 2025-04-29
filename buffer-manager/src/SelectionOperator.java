import java.nio.charset.StandardCharsets;

public class SelectionOperator implements Operator {
    private Operator child;

    public SelectionOperator(Operator child) {
        this.child = child;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Row next() {
        while (true) {
            Row row = child.next();
            if (row == null) {
                return null;
            }
            // category is at offset (9 + 10) = 19, length = 20
            byte[] categoryBytes = row.getAttribute(19, 20);
            String category = new String(categoryBytes, StandardCharsets.UTF_8).trim();
            if (category.equals("director")) {
                return row;
            }
        }
    }

    @Override
    public void close() {
        child.close();
    }
}
