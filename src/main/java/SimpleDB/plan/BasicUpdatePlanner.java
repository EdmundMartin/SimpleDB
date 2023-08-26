package SimpleDB.plan;

import SimpleDB.metadata.MetadataManager;
import SimpleDB.parse.*;
import SimpleDB.query.Constant;
import SimpleDB.query.UpdateScan;
import SimpleDB.transaction.Transaction;

import java.util.Iterator;

public class BasicUpdatePlanner implements UpdatePlanner {

    private final MetadataManager metadataManager;

    public BasicUpdatePlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public int executeInsert(InsertData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.tableName(), metadataManager);
        UpdateScan updateScan = (UpdateScan) plan.open();
        updateScan.insert();
        Iterator<Constant> iterator = data.getValues().iterator();
        for (String fieldName: data.fields()) {
            Constant val = iterator.next();
            updateScan.setVal(fieldName, val);
        }
        updateScan.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.tableName(), metadataManager);
        plan = new SelectPlan(plan, data.predicate());
        UpdateScan updateScan = (UpdateScan) plan.open();
        int count = 0;
        while (updateScan.next()) {
            updateScan.delete();
            count++;
        }
        updateScan.close();
        return count;
    }

    @Override
    public int executeModify(ModifyData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.tableName(), metadataManager);
        plan = new SelectPlan(plan, data.predicate());
        UpdateScan updateScan = (UpdateScan) plan.open();
        int count = 0;
        while (updateScan.next()) {
            Constant value = data.newValue().evaluate(updateScan);
            updateScan.setVal(data.targetField(), value);
            count++;
        }
        updateScan.close();
        return count;
    }

    @Override
    public int executeCreateTable(CreateTableData data, Transaction transaction) {
        metadataManager.createTable(data.tableName(), data.newSchema(), transaction);
        return 0;
    }

    @Override
    public int executeCreateView(CreateViewData data, Transaction transaction) {
        metadataManager.createView(data.viewName(), data.viewDefinition(), transaction);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexData data, Transaction transaction) {
        metadataManager.createIndex(data.indexName(), data.tableName(), data.fieldName(), transaction);
        return 0;
    }
}
