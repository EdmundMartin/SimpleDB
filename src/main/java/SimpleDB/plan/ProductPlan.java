package SimpleDB.plan;

import SimpleDB.query.ProductScan;
import SimpleDB.query.Scan;
import SimpleDB.record.Schema;

public class ProductPlan implements Plan {
    private final Plan first;
    private final Plan second;
    private final Schema schema = new Schema();


    public ProductPlan(Plan first, Plan second) {
        this.first = first;
        this.second = second;
        schema.addAll(first.schema());
        schema.addAll(second.schema());
    }

    @Override
    public Scan open() {
        Scan scanOne = first.open();
        Scan scanTwo = second.open();
        return new ProductScan(scanOne, scanTwo);
    }

    @Override
    public int blocksAccessed() {
        return first.blocksAccessed() + (first.recordsOutput() * second.blocksAccessed());
    }

    @Override
    public int recordsOutput() {
        return first.recordsOutput() * second.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        if (first.schema().hasField(fieldName)) {
            return first.distinctValues(fieldName);
        }
        return second.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
