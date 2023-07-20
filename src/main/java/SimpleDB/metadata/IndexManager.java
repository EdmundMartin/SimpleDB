package SimpleDB.metadata;

import SimpleDB.record.Layout;
import SimpleDB.record.Schema;
import SimpleDB.record.TableScan;
import SimpleDB.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class IndexManager {
    private final Layout layout;
    private final TableManager tableManager;
    private final StatManager statManager;

    public static final String INDEX_CATALOGUE_TABLE_NAME = "idxcat";
    public static final String INDEX_NAME_FIELD = "indexname";
    public static final String INDEX_TABLE_NAME_FIELD = "tablename";
    public static final String INDEX_FIELD_NAME_FIELD = "fieldname";


    public IndexManager(boolean isNew, TableManager tableManager, StatManager statManager, Transaction tx) {
        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField(INDEX_NAME_FIELD, TableManager.MAX_NAME_LENGTH);
            schema.addStringField(INDEX_TABLE_NAME_FIELD, TableManager.MAX_NAME_LENGTH);
            schema.addStringField(INDEX_FIELD_NAME_FIELD, TableManager.MAX_NAME_LENGTH);
            tableManager.createTable(INDEX_CATALOGUE_TABLE_NAME, schema, tx);
        }
        this.tableManager = tableManager;
        this.statManager = statManager;
        this.layout = tableManager.getLayout(INDEX_CATALOGUE_TABLE_NAME, tx);
    }

    /**
     * Create an index of the specified type for the specified field.
     * A unique ID is assigned to this index, and it's information is stored in the idxcat table.
     */
    public void createIndex(String idxName, String tableName, String fieldName, Transaction tx) {
        TableScan tableScan = new TableScan(tx, INDEX_CATALOGUE_TABLE_NAME, layout);
        tableScan.insert();
        tableScan.setString(INDEX_NAME_FIELD, idxName);
        tableScan.setString(INDEX_TABLE_NAME_FIELD, tableName);
        tableScan.setString(INDEX_FIELD_NAME_FIELD, fieldName);
        tableScan.close();
    }

    /**
     * Return a map contain the index info for all indexes on the specified table
     */
    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction tx) {
        Map<String, IndexInfo> result = new HashMap<>();
        TableScan tableScan = new TableScan(tx, INDEX_CATALOGUE_TABLE_NAME, layout);

        while (tableScan.next()) {
            if (tableScan.getString(INDEX_TABLE_NAME_FIELD).equals(tableName)) {
                String idxName = tableScan.getString(INDEX_NAME_FIELD);
                String fieldName = tableScan.getString(INDEX_FIELD_NAME_FIELD);
                Layout tableLayout = tableManager.getLayout(tableName, tx);
                StatInfo tableStats = statManager.getStatInfo(tableName, tableLayout, tx);
                IndexInfo info = new IndexInfo(idxName, fieldName, tableLayout.schema(), tx, tableStats);
            }
        }
        tableScan.close();
        return result;
    }
}
