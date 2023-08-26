package SimpleDB.plan;

import SimpleDB.parse.*;
import SimpleDB.query.UpdateScan;
import SimpleDB.transaction.Transaction;

public class Planner {

    private QueryPlanner queryPlanner;
    private UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan createQueryPlan(String query, Transaction transaction) {
        Parser parser = new Parser(query);
        QueryData data = parser.query();
        verifyQuery(data);
        return queryPlanner.createPlan(data, transaction);
    }

    public int executeUpdate(String command, Transaction transaction) {
        Parser parser = new Parser(command);
        Object data = parser.updateCmd();
        verifyUpdate(data);

        if (data instanceof InsertData) {
            return updatePlanner.executeInsert((InsertData) data, transaction);
        }
        if (data instanceof DeleteData) {
            return updatePlanner.executeDelete((DeleteData) data, transaction);
        }
        if (data instanceof ModifyData) {
            return updatePlanner.executeModify((ModifyData) data, transaction);
        }
        if (data instanceof CreateTableData) {
            return updatePlanner.executeCreateTable((CreateTableData) data, transaction);
        }
        if (data instanceof CreateViewData) {
            return updatePlanner.executeCreateView((CreateViewData) data, transaction);
        }
        if (data instanceof CreateIndexData) {
            return updatePlanner.executeCreateIndex((CreateIndexData) data, transaction);
        }
        return 0;
    }

    // SimpleDB does not verify queries, although it should.
    private void verifyQuery(QueryData data) {
    }

    // SimpleDB does not verify updates, although it should
    private void verifyUpdate(Object data) {
    }
}
