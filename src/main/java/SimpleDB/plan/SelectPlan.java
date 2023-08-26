package SimpleDB.plan;

import SimpleDB.query.Predicate;
import SimpleDB.query.Scan;
import SimpleDB.query.SelectScan;
import SimpleDB.record.Schema;

public class SelectPlan implements Plan {
    private final Plan plan;
    private final Predicate predicate;


    public SelectPlan(Plan plan, Predicate predicate) {
        this.plan = plan;
        this.predicate = predicate;
    }

    public Scan open() {
        Scan subScan = plan.open();
        return new SelectScan(subScan, predicate);
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput() / predicate.reductionFactor(plan);
    }

    @Override
    public int distinctValues(String fieldName) {
        if (predicate.equatesWithConstant(fieldName) != null) {
            return 1;
        }
        String otherField = predicate.equatesWithField(fieldName);
        if (otherField != null) {
            return Math.min(plan.distinctValues(fieldName), plan.distinctValues(otherField));
        }
        return plan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return null;
    }


}
