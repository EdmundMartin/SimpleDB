package SimpleDB.query;

import SimpleDB.record.RID;

public interface UpdateScan extends Scan {

    /**
     * Modify the field value of the current record.
     * @param fieldName the name of the field.
     * @param value the new value, expressed as a Constant
     */
    public void setVal(String fieldName, Constant value);


    /**
     * Modify the field value of the current record.
     * @param fieldName the name of the field.
     * @param value the new value, as an integer
     */
    public void setInt(String fieldName, int value);

    /**
     * Modify the field value of the current record
     * @param fieldName the name of the field
     * @param value the value as a String
     */
    public void setString(String fieldName, String value);

    /**
     * Insert a new record somewhere in the scan
     */
    public void insert();

    /**
     * Delete the current record from the scan.
     */
    public void delete();

    /**
     * Return the id of the current record.
     * @return the id of the current record
     */
    public RID getRid();

    /**
     * Position the scan so that the current record has
     * the specified id.
     * @param rid the id of the desired record
     */
    public void moveToRid(RID rid);
}
