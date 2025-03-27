import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
public class Row {

    public byte[] data;

    public Row(byte[]... columns) {
        if (columns.length == 1) {
            data = columns[0];
        } else {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            try {
                for (byte[] col : columns) {
                    stream.write(col);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            data = stream.toByteArray();
        }
    }

    @Override
    public String toString() {
        return new String(data, StandardCharsets.UTF_8);
    }

}