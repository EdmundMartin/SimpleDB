package SimpleDB.record;

import SimpleDB.file.BlockId;
import SimpleDB.transaction.Transaction;

import static java.sql.Types.INTEGER;

public class RecordPage {
    public static final int EMPTY = 0;
    public static final int USED = 1;

    private Transaction tx;
    private BlockId blk;
    private Layout layout;

    public RecordPage(Transaction tx, BlockId block, Layout layout) {
        this.tx = tx;
        this.blk = block;
        this.layout = layout;
        tx.pin(block);
    }

    public int getInt(int slot, String fieldName) {
        int fieldPos = offset(slot) + layout.offset(fieldName);
        return tx.getInt(blk, fieldPos);
    }

    public String getString(int slot, String fieldName) {
        int fieldPos = offset(slot) + layout.offset(fieldName);
        return tx.getString(blk, fieldPos);
    }

    /**
     * Store an integer at the specified field
     * of the specified slot.
     */
    public void setInt(int slot, String fieldName, int value) {
        int fieldPos = offset(slot) + layout.offset(fieldName);
        tx.setInt(blk, fieldPos, value, true);
    }

    /**
     * Store a string at the specified field
     * of the specified slot
     */
    public void setString(int slot, String fieldName, String value) {
        int fieldPos = offset(slot) + layout.offset(fieldName);
        tx.setString(blk, fieldPos, value, true);
    }


    /**
     * Use the layout to format a new block of records
     * These values should not be logged
     * (because the old values are meaningless)
     */
    public void format() {
        int slot = 0;
        while (isValidSlot(slot)) {
            tx.setInt(blk, offset(slot), EMPTY, false);
            Schema sch = layout.schema();
            for (String fieldName : sch.fields()) {
                int fieldPos = offset(slot) + layout.offset(fieldName);
                if (sch.type(fieldName) == INTEGER) {
                    tx.setInt(blk, fieldPos, 0, false);
                } else {
                    tx.setString(blk, fieldPos, "", false);
                }
            }
        }
    }

    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, EMPTY);
        if (newSlot >= 0) {
            setFlag(newSlot, USED);
        }
        return newSlot;
    }

    public BlockId block() {
        return blk;
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    private void setFlag(int slot, int flag) {
        tx.setInt(blk, offset(slot), flag, true);
    }

    private int searchAfter(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (tx.getInt(blk, offset(slot)) == flag) {
                return slot;
            }
            slot++;
        }
        return -1;
    }

    private boolean isValidSlot(int slot) {
        return offset(slot + 1) <= tx.blockSize();
    }

    private int offset(int slot) {
        return slot * layout.slotSize();
    }
}
