public interface Operator {
    void open();
    Row next();
    void close();
}