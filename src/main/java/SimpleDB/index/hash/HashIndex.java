package SimpleDB.index.hash;

import SimpleDB.index.Index;
import SimpleDB.query.Constant;
import SimpleDB.record.Layout;
import SimpleDB.record.RID;
import SimpleDB.record.TableScan;
import SimpleDB.transaction.Transaction;

public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;
    private static final String DATA_VALUE_FIELD = "dataval";
    private static final String BLOCK_FIELD = "block";
    private static final String ID_FIELD = "id";
    private Transaction tx;
    private String idxName;
    private Layout layout;
    private Constant searchKey = null;
    private TableScan tableScan = null;

    public HashIndex(Transaction tx, String idxName, Layout layout) {
        this.tx = tx;
        this.idxName = idxName;
        this.layout = layout;
    }

    @Override
    public void beforeFirst(Constant searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKETS;
        String tableName = idxName + bucket;
        this.tableScan = new TableScan(tx, tableName, layout);
    }

    @Override
    public boolean next() {
        while (tableScan.next()) {
            if (tableScan.getVal(DATA_VALUE_FIELD).equals(searchKey)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RID getDataRid() {
        int blockNumber = tableScan.getInt(BLOCK_FIELD);
        int id = tableScan.getInt(ID_FIELD);
        return new RID(blockNumber, id);
    }

    @Override
    public void insert(Constant dataValue, RID dataRid) {
        beforeFirst(dataValue);
        tableScan.insert();
        tableScan.setInt(BLOCK_FIELD, dataRid.blockNumber());
        tableScan.setInt(ID_FIELD, dataRid.slot());
        tableScan.setVal(DATA_VALUE_FIELD, dataValue);
    }

    @Override
    public void delete(Constant dataValue, RID dataRid) {
        beforeFirst(dataValue);
        while (next()) {
            if (getDataRid().equals(dataRid)) {
                tableScan.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if (tableScan != null) {
            tableScan.close();
        }
    }

    /**
     * Returns the cost of searching an index file having the specified number of blocks.
     * The method assumes that all buckets are about the same size, and so the cost is simply the size
     * of the bucket.
     */
    public static int searchCost(int numberBlocks, int rpb) {
        return numberBlocks / HashIndex.NUM_BUCKETS;
    }
}
