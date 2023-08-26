package SimpleDB.plan;

import SimpleDB.metadata.MetadataManager;
import SimpleDB.metadata.StatInfo;
import SimpleDB.query.Scan;
import SimpleDB.record.Layout;
import SimpleDB.record.Schema;
import SimpleDB.record.TableScan;
import SimpleDB.transaction.Transaction;

public class TablePlan implements Plan {
    private final String tableName;
    private final Transaction transaction;
    private final Layout layout;
    private final StatInfo statInfo;


    public  TablePlan(Transaction transaction, String tableName, MetadataManager metadataManager) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.layout = metadataManager.getLayout(tableName, transaction);
        this.statInfo = metadataManager.getStatInfo(tableName, layout, transaction);
    }

    public Scan open() {
        return new TableScan(transaction, tableName, layout);
    }

    public int blocksAccessed() {
        return statInfo.blocksAccessed();
    }

    public int recordsOutput() {
        return statInfo.recordsOutput();
    }

    public int distinctValues(String fieldName) {
        return statInfo.distinctValues(fieldName);
    }

    public Schema schema() {
        return layout.schema();
    }
}
