package SimpleDB.metadata;

import SimpleDB.record.Layout;
import SimpleDB.record.Schema;
import SimpleDB.record.TableScan;
import SimpleDB.transaction.Transaction;

public class ViewManager {

    private static final int MAX_VIEW_DEFINITION_LEN = 100;

    private static final String VIEW_CATALOGUE_TABLE_NAME = "viewcat";
    private static final String VIEW_NAME_FIELD = "viewname";
    private static final String VIEW_DEFINITION_FIELD = "viewdef";

    TableManager tableManager;

    public ViewManager(boolean isNew, TableManager tableManager, Transaction tx) {
        this.tableManager = tableManager;
        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField(VIEW_NAME_FIELD, TableManager.MAX_NAME_LENGTH);
            schema.addStringField(VIEW_DEFINITION_FIELD, MAX_VIEW_DEFINITION_LEN);
            tableManager.createTable(VIEW_CATALOGUE_TABLE_NAME, schema, tx);
        }
    }

    public void createView(String viewName, String viewDefinition, Transaction tx) {
        Layout layout = tableManager.getLayout(VIEW_CATALOGUE_TABLE_NAME, tx);
        TableScan tableScan = new TableScan(tx, VIEW_CATALOGUE_TABLE_NAME, layout);
        tableScan.insert();
        tableScan.setString(VIEW_NAME_FIELD, viewName);
        tableScan.setString(VIEW_DEFINITION_FIELD, viewDefinition);
        tableScan.close();
    }

    public String getViewDefinition(String viewName, Transaction tx) {
        String result = null;
        Layout layout = tableManager.getLayout(VIEW_CATALOGUE_TABLE_NAME, tx);
        TableScan tableScan = new TableScan(tx, VIEW_CATALOGUE_TABLE_NAME, layout);

        while (tableScan.next()) {
            if (tableScan.getString(VIEW_NAME_FIELD).equals(viewName)) {
                result = tableScan.getString(VIEW_DEFINITION_FIELD);
                break;
            }
        }
        tableScan.close();
        return result;
    }
}
