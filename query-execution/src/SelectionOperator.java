import java.nio.ByteBuffer;

public class SelectionOperator implements Operator {

    private Operator child;
    private int[] attr;
    private int rowLength;
    private ByteBuffer low;
    private ByteBuffer high;

    public SelectionOperator(Operator child, int[] attr, String low, String high) {
        this.child = child;
        this.attr = attr;
        rowLength = attr[1] - attr[0];
        this.low = ByteKeyBTree.toComparableBytes(low, rowLength);
        this.high = ByteKeyBTree.toComparableBytes(high, rowLength);
    }

    public SelectionOperator(Operator child, int[] attr, String exact) {
        this.child = child;
        this.attr = attr;
        rowLength = attr[1] - attr[0];
        low = ByteKeyBTree.toComparableBytes(exact, rowLength);
        high = low;
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
            ByteBuffer inChild = ByteKeyBTree.toComparableBytes(row.getString(attr), rowLength);
            if (low.compareTo(inChild) <= 0 && inChild.compareTo(high) <= 0) { // 
                return row;
            }
        }
    }

    @Override
    public void close() {
        child.close();
    }

}
