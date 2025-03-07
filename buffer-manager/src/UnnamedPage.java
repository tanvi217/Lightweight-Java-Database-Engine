import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class UnnamedPage implements Page {
    private int currRowId;
    private int size = 4096;
    //ends up being 105 entries per page, so when currRowId >= 106 the page is full
    int pageId;
    
    public UnnamedPage(int pageId){
        currRowId = 1;
        this.pageId = pageId;
    }

    @Override
    public Row getRow(int rowId) {

        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int insertRow(Row row) {
        //MAKE SURE TO PAD ENTIRES SO TITLE IS EXACTLY 30 characters, or find another solution
        //trying a very basic write
        String filePath = "buffer-manager\\data\\data.bin";
        // the true means that we are in "append" mode rather than overwrite mode, which is what we want
        try(DataOutputStream dos = new DataOutputStream(new FileOutputStream(filePath, true))){
            for (Byte b : row.movieId){
                dos.write(b);
            }
            for (Byte b : row.title){
                dos.write(b);
            }
        }catch(IOException err){
            err.printStackTrace();
        }
        return currRowId++;
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isFull() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getId() {
        return pageId;
    }

    @Override
    public byte[] serialize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'serialize'");
    }

    @Override
    public Page deserialize(byte[] data) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }
}
