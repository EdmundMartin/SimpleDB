package SimpleDB.plan;

import SimpleDB.metadata.MetadataManager;
import SimpleDB.parse.Parser;
import SimpleDB.parse.QueryData;
import SimpleDB.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

public class BasicQueryPlanner implements QueryPlanner {
    private final MetadataManager metadataManager;

    public BasicQueryPlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public Plan createPlan(QueryData data, Transaction transaction) {
        // Step 1: Create plan for each mentioned table or view
        List<Plan> plans = new ArrayList<>();

        for (String tableName: data.tables()) {
            String viewDefinition = metadataManager.getViewDef(tableName, transaction);
            if (viewDefinition != null) {
                Parser parser = new Parser(viewDefinition);
                QueryData viewData = parser.query();
                plans.add(createPlan(viewData, transaction));
            } else {
                plans.add(new TablePlan(transaction, tableName, metadataManager));
            }
        }

        // Step 2: Create the product of all table plans
        Plan plan = plans.remove(0);
        for (Plan nextPlan: plans) {
            plan = new ProductPlan(plan, nextPlan);
        }

        // Step 3: Add a selection plan for the predicate
        plan = new SelectPlan(plan, data.predicate());

        // Step 4: Project on the field names
        plan = new ProjectPlan(plan, data.fields());
        return plan;
    }
}
