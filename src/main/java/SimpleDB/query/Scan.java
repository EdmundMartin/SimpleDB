package SimpleDB.query;

public interface Scan {

    /**
     * Position the scan before its first record.
     * A subsequent call to next() will return the first record.
     */
    public void beforeFirst();

    /**
     * Move the scan to the next record
     * @return false if there is no next record
     */
    public boolean next();

    /**
     * Return the value of the specified integer field
     * in the current record.
     * @param fieldName the name of the field
     * @return the field's integer value in the current record
     */
    public int getInt(String fieldName);


    /**
     * Return the value of the specified string field
     * in the current record.
     * @param fieldName the name of the field
     * @return the field's string value in the current record
     */
    public String getString(String fieldName);

    /**
     * Return the value of a the specified field in th current record.
     * The value is expressed as a Constant.
     * @param fieldName the name of the field
     * @return the value of that field, expressed as a Constant
     */
    public Constant getVal(String fieldName);

    /**
     * Return true if the scan has the specified field.
     * @param fieldName the name of the field
     * @return true if the scan has that field
     */
    public boolean hasField(String fieldName);

    /**
     * Close the scan and it's subscans, if any
     */
    public void close();
}
