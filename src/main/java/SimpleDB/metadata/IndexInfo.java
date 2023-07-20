package SimpleDB.metadata;

import SimpleDB.index.Index;
import SimpleDB.index.hash.HashIndex;
import SimpleDB.record.Layout;
import SimpleDB.record.Schema;
import SimpleDB.transaction.Transaction;

import static java.sql.Types.INTEGER;

public class IndexInfo {
    private final String idxName;
    private final String fieldName;
    private final Transaction transaction;
    private final Schema tableSchema;
    private final Layout idxLayout;
    private final StatInfo statInfo;

    private final static String BLOCK_FIELD = "block";
    private final static String ID_FIELD = "id";
    private final static String DATA_VALUE_FIELD = "dataval";

    public IndexInfo(String idxName, String fieldName, Schema tableSchema, Transaction tx, StatInfo statInfo) {
        this.idxName = idxName;
        this.fieldName = fieldName;
        this.transaction = tx;
        this.tableSchema = tableSchema;
        this.idxLayout = createIdxLayout();
        this.statInfo = statInfo;
    }

    public Index open() {
        return new HashIndex(transaction, idxName, idxLayout);
    }

    /**
     * Estimate the number of block accesses required to fina all index records having a particular search key.
     * The method uses the table's metadata to estimate the size of the index file and the number of index records
     * per block.
     */
    public int blocksAccessed() {
        int rowPerBlock = transaction.blockSize() / idxLayout.slotSize();
        int numberBlocks = statInfo.recordsOutput() / rowPerBlock;
        return HashIndex.searchCost(numberBlocks, rowPerBlock);
    }

    /**
     * Return the estimated number of records having a search keu. This value is the same as doing a select query;
     * that is, the number of records in the table divided by the number of distinct values of the indexed field.
     */
    public int recordsOutput() {
        return statInfo.recordsOutput() / statInfo.distinctValues(fieldName);
    }

    public int distinctValues(String fname) {
        return fname.equals(this.fieldName) ? 1 : statInfo.distinctValues(this.fieldName);
    }


    private Layout createIdxLayout() {
        Schema schema = new Schema();
        schema.addIntField(BLOCK_FIELD);
        schema.addIntField(ID_FIELD);
        if (tableSchema.type(fieldName) == INTEGER) {
            schema.addIntField(DATA_VALUE_FIELD);
        } else {
            int fieldLength = tableSchema.length(fieldName);
            schema.addStringField(DATA_VALUE_FIELD, fieldLength);
        }
        return new Layout(schema);
    }
}
