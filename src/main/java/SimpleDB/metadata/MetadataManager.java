package SimpleDB.metadata;

import SimpleDB.record.Layout;
import SimpleDB.record.Schema;
import SimpleDB.transaction.Transaction;

import java.util.Map;

public class MetadataManager {

    private static TableManager tableManager;
    private static ViewManager viewManager;
    private static StatManager statManager;
    private static IndexManager indexManager;


    public MetadataManager(boolean isNew, Transaction tx) {
        tableManager = new TableManager(isNew, tx);
        viewManager = new ViewManager(isNew, tableManager, tx);
        statManager = new StatManager(tableManager, tx);
        indexManager = new IndexManager(isNew, tableManager, statManager, tx);
    }

    public void createTable(String tableName, Schema schema, Transaction tx) {
        tableManager.createTable(tableName, schema, tx);
    }

    public Layout getLayout(String tableName, Transaction transaction) {
        return tableManager.getLayout(tableName, transaction);
    }

    public void createView(String viewName, String viewDef, Transaction tx) {
        viewManager.createView(viewName, viewDef, tx);
    }

    public String getViewDef(String viewName, Transaction tx) {
        return viewManager.getViewDefinition(viewName, tx);
    }

    public void createIndex(String idxName, String tableName, String fieldName, Transaction tx) {
        indexManager.createIndex(idxName, tableName, fieldName, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction tx) {
        return indexManager.getIndexInfo(tableName, tx);
    }

    public StatInfo getStatInfo(String tableName, Layout layout, Transaction tx) {
        return statManager.getStatInfo(tableName, layout, tx);
    }
}
