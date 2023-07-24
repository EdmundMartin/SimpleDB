package SimpleDB.query;

import java.util.List;

/**
 * The scan class corresponding the project relational algebra operator. All methods expect hasField delegate their
 * work to the underlying scan.
 */
public class ProjectScan implements Scan {
    private final Scan scan;
    private final List<String> fieldList;

    /**
     * Create a project scan having the specified underlying scan and field list.
     */
    public ProjectScan(Scan scan, List<String> fieldList) {
        this.scan = scan;
        this.fieldList = fieldList;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        return scan.next();
    }

    @Override
    public int getInt(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getInt(fieldName);
        }
        throw new RuntimeException("field " + fieldName + " not found.");
    }

    @Override
    public String getString(String fieldName) {
       if (hasField(fieldName)) {
           return scan.getString(fieldName);
       }
       throw new RuntimeException("field " + fieldName + " not found.");
    }

    @Override
    public Constant getVal(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getVal(fieldName);
        }
        throw new RuntimeException("field " + fieldName + " not found.");

    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }
}
