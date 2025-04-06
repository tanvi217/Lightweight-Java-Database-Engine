import java.nio.ByteBuffer;

public class Rid {
    private int pageId;
    private int slotId;

    public Rid(int pageId, int slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }

    public Rid(byte[] eightBytes) {
        ByteBuffer twoInts = ByteBuffer.wrap(eightBytes);
        twoInts.position(0); // is this needed?
        pageId = twoInts.getInt();
        slotId = twoInts.getInt();
    }

    public int getPageId() {
        return pageId;
    }

    public int getSlotId() {
        return slotId;
    }

    @Override
    public String toString() {
        //print the Rid in the format (pageId, slotId)
        return "(" + pageId + ", " + slotId + ")";
    }
}
