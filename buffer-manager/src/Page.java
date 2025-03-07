public interface Page {

    /**
     * Fetches a row from the page by its row ID.
     *
     * @param rowId The ID of the row to retrieve.
     * @return The Row object containing the requested data.
     */
    Row getRow(int rowId);

    /**
     * Inserts a new row into the page.
     *
     * @param row The Row object containing the data to insert.
     * @return The row ID of the inserted row, or -1 if the page is full
     */
    int insertRow(Row row);

    /**
     * Check if the page is full.
     *
     * @return true if the page is full, false otherwise
     */
    boolean isFull();

    /**
     * @return the pageId of this page, used in BufferManager
     */
    int getId();

    /**
    * TODO: Serialize this page into a byte array.
     * @return the serialized byte array of this page
     */
    byte[] serialize();

    /**
     * TODO: Deserialize a byte array into a Page object.
     *
     * @param pageId The ID of the page to deserialize.
     * @param data The byte array to deserialize.
     * @return The Page object containing the deserialized data.
     */
    Page deserialize(byte[] data);
}
