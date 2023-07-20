package SimpleDB.index;

import SimpleDB.query.Constant;
import SimpleDB.record.RID;

public interface Index {

    /**
     * Positions the index before the first record
     * having the specified search key.
     * @param searchKey
     */
    public void beforeFirst(Constant searchKey);

    /**
     * Moves the index to the next record having the search key specified in the beforeFirst
     * method.
     * @return false if no other index records have the search key.
     */
    public boolean next();

    /**
     * Returns the dataRID value stored in the current index record.
     * @return the dataRID stored in the current index record.
     */
    public RID getDataRid();


    /**
     * Inserts an index redord having the specified data value and the dataRID values.
     */
    public void insert(Constant dataValue, RID dataRid);

    /**
     * Deletes the index record having the specified data value and data RID values.
     */
    public void delete(Constant dataValue, RID dataRid);


    public void close();
}
