public abstract class Schema {

    public String fileTitle;
    public int[][] ranges;
    public int length;
    private BufferManager bm;

    public Schema(String fileTitle, int[][] ranges, BufferManager bm) {
        this.fileTitle = fileTitle;
        this.ranges = ranges;
        length = ranges[ranges.length - 1][1];
        this.bm = bm;
    }

    public Page createPage() {
        return bm.createPage(fileTitle, length);
    }

    public Page getPage(int pid) {
        return bm.getPage(pid, fileTitle);
    }

    public void unpinPage(int pid) {
        bm.unpinPage(pid, fileTitle);
    }

    public void markDirty(int pid) {
        bm.markDirty(pid, fileTitle);
    }

}
