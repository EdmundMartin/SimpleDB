package SimpleDB.plan;

import SimpleDB.query.Scan;
import SimpleDB.record.Schema;

public interface Plan {
    public Scan open();
    public int blocksAccessed();
    public int recordsOutput();
    public int distinctValues(String fieldName);
    public Schema schema();
}

