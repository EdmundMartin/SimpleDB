package SimpleDB.plan;

import SimpleDB.parse.QueryData;
import SimpleDB.transaction.Transaction;

public interface QueryPlanner {

    public Plan createPlan(QueryData data, Transaction transaction);
}
