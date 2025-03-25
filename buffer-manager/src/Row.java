import java.nio.charset.StandardCharsets;
public class Row {

    public byte[] data;

    public Row(byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return new String(data, StandardCharsets.UTF_8);
    }

}