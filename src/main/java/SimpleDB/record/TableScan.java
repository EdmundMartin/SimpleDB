package SimpleDB.record;

import SimpleDB.file.BlockId;
import SimpleDB.query.Constant;
import SimpleDB.query.UpdateScan;
import SimpleDB.transaction.Transaction;
import static java.sql.Types.INTEGER;

public class TableScan implements UpdateScan {
    private final Transaction tx;
    private final Layout layout;
    private RecordPage recordPage;
    private final String fileName;
    private int currentSlot;

    public TableScan(Transaction tx, String tableName, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        this.fileName = tableName + ".tbl";
        if (tx.size(fileName) == 0) {
            // Move to next block - if page is empty
            moveToNewBlock();
            return;
        }
        moveToBlock(0);
    }

    @Override
    public void setVal(String fieldName, Constant value) {
        if (layout.schema().type(fieldName) == INTEGER) {
            setInt(fieldName, value.asInt());
            return;
        }
        setString(fieldName, value.asString());
    }

    @Override
    public void setInt(String fieldName, int value) {
        recordPage.setInt(currentSlot, fieldName, value);
    }

    @Override
    public void setString(String fieldName, String value) {
        recordPage.setString(currentSlot, fieldName, value);
    }

    @Override
    public void insert() {
        currentSlot = recordPage.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(recordPage.block().number()+1);
            }
            currentSlot = recordPage.insertAfter(currentSlot);
        }
    }

    @Override
    public void delete() {
        recordPage.delete(currentSlot);
    }

    @Override
    public RID getRid() {
        return new RID(recordPage.block().number(), currentSlot);
    }

    @Override
    public void moveToRid(RID rid) {
        close();
        BlockId block = new BlockId(fileName, rid.blockNumber());
        recordPage = new RecordPage(tx, block, layout);
        currentSlot = rid.slot();
    }

    @Override
    public void beforeFirst() {
        moveToBlock(0);
    }

    @Override
    public boolean next() {
        currentSlot = recordPage.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false;
            }
            moveToBlock(recordPage.block().number()+1);
            currentSlot = recordPage.nextAfter(currentSlot);
        }
        return true;
    }

    @Override
    public int getInt(String fieldName) {
        return recordPage.getInt(currentSlot, fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return recordPage.getString(currentSlot, fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        if (layout.schema().type(fieldName) == INTEGER) {
            return new Constant(getInt(fieldName));
        }
        return new Constant(getString(fieldName));
    }

    @Override
    public boolean hasField(String fieldName) {
        return layout.schema().hasField(fieldName);
    }

    @Override
    public void close() {
        if (recordPage != null) {
            tx.unpin(recordPage.block());
        }
    }

    private void moveToBlock(int blockNum) {
        close();
        BlockId block = new BlockId(fileName, blockNum);
        recordPage = new RecordPage(tx, block, layout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId block = tx.append(fileName);
        recordPage = new RecordPage(tx, block, layout);
        recordPage.format();
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return recordPage.block().number() == tx.size(fileName) - 1;
    }
}
