package SimpleDB.metadata;

import SimpleDB.record.Layout;
import SimpleDB.record.TableScan;
import SimpleDB.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class StatManager {
    private TableManager tableManager;
    private Map<String, StatInfo> tableStats;
    private int numberOfCalls;

    private static final String TABLE_CATALOGUE_NAME = "tblcat";
    private static final String TABLE_NAME_FIELD = "tblname";

    public StatManager(TableManager tableManager, Transaction transaction) {
        this.tableManager = tableManager;
        refreshStatistics(transaction);
    }

    /**
     * Return the statistical information about the specified table.
     */
    public synchronized StatInfo getStatInfo(String tableName, Layout layout, Transaction tx) {
        numberOfCalls++;
        if (numberOfCalls > 100) {
            refreshStatistics(tx);
        }
        StatInfo statsInfo = tableStats.get(tableName);
        if (statsInfo == null) {
            statsInfo = calcTableStats(tableName, layout, tx);
            tableStats.put(tableName, statsInfo);
        }
        return statsInfo;
    }

    public synchronized void refreshStatistics(Transaction tx) {
        tableStats = new HashMap<>();
        numberOfCalls = 0;
        Layout tableCatalogueLayout = tableManager.getLayout(TABLE_CATALOGUE_NAME, tx);
        TableScan tableScan = new TableScan(tx, TABLE_CATALOGUE_NAME, tableCatalogueLayout);

        while (tableScan.next()) {
            String tableName = tableScan.getString(TABLE_NAME_FIELD);
            Layout layout = tableManager.getLayout(tableName, tx);
            StatInfo statInfo = calcTableStats(tableName, layout, tx);
            tableStats.put(tableName, statInfo);
        }
        tableScan.close();
    }

    private synchronized StatInfo calcTableStats(String tableName, Layout layout, Transaction tx) {
        int numberOfRecords = 0;
        int numberOfBlocks = 0;
        TableScan tableScan = new TableScan(tx, tableName, layout);

        while (tableScan.next()) {
            numberOfRecords++;
            numberOfBlocks = tableScan.getRid().blockNumber() + 1;
        }
        tableScan.close();
        return new StatInfo(numberOfBlocks, numberOfRecords);
    }
}
