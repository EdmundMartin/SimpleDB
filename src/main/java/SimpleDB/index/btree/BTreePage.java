package SimpleDB.index.btree;

import SimpleDB.file.BlockId;
import SimpleDB.query.Constant;
import SimpleDB.record.Layout;
import SimpleDB.record.Schema;
import SimpleDB.transaction.Transaction;

import static java.sql.Types.INTEGER;


/**
 * BTree directory and leaf pages have many commonalities
 * in particular, their records are stored in sorted order,
 * and pages split when full.
 * A BTNode object contains this common functionality
 */
public class BTreePage {
    private final Transaction transaction;
    private final Layout layout;
    private BlockId currentBlock;

    /**
     * Open a BTNode for the specified B-Tree block
     */
    public BTreePage(Transaction tx, BlockId currentBlock, Layout layout) {
        this.transaction = tx;
        this.currentBlock = currentBlock;
        this.layout = layout;
        tx.pin(currentBlock);
    }

    /**
     * Delete the index record at the specified slot
     */
    public void delete(int slot) {
        for (int i = slot + 1; i < getNumberOfRecords(); i++) {
            copyRecord(i, i-1);
        }
        setNumberOfRecords(getNumberOfRecords()-1);
    }

    /**
     * Return the number of index records in this page.
     */
    public int getNumberOfRecords() {
        return transaction.getInt(currentBlock, Integer.BYTES);
    }

    private int getInt(int slot, String fieldName) {
        int position = fieldPosition(slot, fieldName);
        return transaction.getInt(currentBlock, position);
    }

    private String getString(int slot, String fieldName) {
        int position = fieldPosition(slot, fieldName);
        return transaction.getString(currentBlock, position);
    }


    private Constant getValue(int slot, String fieldName) {
        int type = layout.schema().type(fieldName);
        if (type == INTEGER) {
            return new Constant(getInt(slot, fieldName));
        }
        return new Constant(getString(slot, fieldName));
    }

    private void setInt(int slot, String fieldName, int value) {
        int position = fieldPosition(slot, fieldName);
        transaction.setInt(currentBlock, position, value, true);
    }

    private void setString(int slot, String fieldName, String value) {
        int position = fieldPosition(slot, fieldName);
        transaction.setString(currentBlock, position, value, true);
    }

    private void setVal(int slot, String fieldName, Constant value) {
        int type = layout.schema().type(fieldName);
        if (type == INTEGER) {
            setInt(slot, fieldName, value.asInt());
            return;
        }
        setString(slot, fieldName, value.asString());
    }

    private void setNumberOfRecords(int n) {
        transaction.setInt(currentBlock, Integer.BYTES, n, true);
    }

    private void insert(int slot) {
        for (int i = getNumberOfRecords(); i > slot; i--) {
            copyRecord(i-1, i);
        }
        setNumberOfRecords(getNumberOfRecords()+1);
    }

    private void copyRecord(int from, int to) {
        Schema schema = layout.schema();
        for (String fieldName: schema.fields()) {
            setVal(to, fieldName, getValue(from, fieldName));
        }
    }

    private void transferRecords(int slot, BTreePage destination) {
        int destinationSlot = 0;
        while (slot < getNumberOfRecords()) {
            destination.insert(destinationSlot);
            Schema schema = layout.schema();
            for (String fieldName: schema.fields()) {
                destination.setVal(destinationSlot, fieldName, getValue(slot, fieldName));
            }
            delete(slot);
            destinationSlot++;
        }
    }

    private int fieldPosition(int slot, String fieldName) {
        int offset = layout.offset(fieldName);
        return slotPosition(slot) + offset;
    }

    private int slotPosition(int slot) {
        int slotSize = layout.slotSize();
        return Integer.BYTES + Integer.BYTES + (slot * slotSize);
    }
}
