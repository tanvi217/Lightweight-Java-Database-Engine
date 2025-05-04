public class SelectionOperator implements Operator {

    private Operator child;
    private int[] attr;
    private String low;
    private String high;

    public SelectionOperator(Operator child, int[] attr, String low, String high) {
        this.child = child;
        this.attr = attr;
        this.low = ByteKeyBTree.toASCII(low.toLowerCase());
        this.high = ByteKeyBTree.toASCII(high.toLowerCase());
    }

    public SelectionOperator(Operator child, int[] attr, String exact) {
        this(child, attr, exact, exact);
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
            String inChild = ByteKeyBTree.toASCII(row.getString(attr).toLowerCase());
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
