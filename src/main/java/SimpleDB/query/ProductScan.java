package SimpleDB.query;

import java.util.List;


/**
 * The scan class corresponding to the product relational algebra operator
 */
public class ProductScan implements Scan {
    private final Scan first;
    private final Scan second;

    public ProductScan(Scan first, Scan second) {
        this.first = first;
        this.second = second;
        beforeFirst();
    }

    /**
     * Position the scan before its first record.
     * In particular, the LHS scan is positioned at its first record, and the RHS scan is positioned before
     * it's first record.
     */
    @Override
    public void beforeFirst() {
        first.beforeFirst();
        first.next();
        second.beforeFirst();
    }

    /**
     * Move the scan to the next record.
     * The method moves to the next RHS record. Otherwise, it moves to the next Left Hand record and the first RHS
     * record. If there are no more left hand records method returns false.
     */
    @Override
    public boolean next() {
        if (second.next()) {
            return true;
        }
        second.beforeFirst();
        return second.next() && first.next();
    }

    /**
     * Return the integer value of the specified field. The value is obtained from whichever scan contains the field.
     */
    @Override
    public int getInt(String fieldName) {
        // TODO - Should really check schema here? Not done in the books version of the code
        if (first.hasField(fieldName)) {
            return first.getInt(fieldName);
        }
        return second.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        if (first.hasField(fieldName)) {
            return first.getString(fieldName);
        }
        return second.getString(fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        if (first.hasField(fieldName)) {
            return first.getVal(fieldName);
        }
        return second.getVal(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return first.hasField(fieldName) || second.hasField(fieldName);
    }

    @Override
    public void close() {
        first.close();
        second.close();
    }
}
