package SimpleDB.plan;

import SimpleDB.query.ProjectScan;
import SimpleDB.query.Scan;
import SimpleDB.record.Schema;

import java.util.List;

public class ProjectPlan implements Plan {

    private final Plan plan;
    private final Schema schema = new Schema();

    public ProjectPlan(Plan plan, List<String> fieldList) {
        this.plan = plan;
        for (String fieldName: fieldList) {
            schema.add(fieldName, plan.schema());
        }
    }

    @Override
    public Scan open() {
        Scan scan = plan.open();
        return new ProjectScan(scan, schema.fields());
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return plan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
