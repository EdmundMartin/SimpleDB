package SimpleDB.transaction.recovery;

import SimpleDB.file.BlockId;
import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.transaction.Transaction;

public class SetIntRecord implements LogRecord {
    private int txNum;
    private int offset;
    private int val;
    private BlockId block;

    public SetIntRecord(Page page) {
        int txPos = Integer.BYTES;
        txNum = page.getInt(txPos);
        int filePos = txPos + Integer.BYTES;
        String filename = page.getString(filePos);

        int blockPos = filePos + Page.maxLength(filename.length());
        int blockNum = page.getInt(blockPos);

        block = new BlockId(filename, blockNum);

        int offsetPos = blockPos + Integer.BYTES;
        offset = page.getInt(offsetPos);

        int valuePos = offsetPos + Integer.BYTES;
        val = page.getInt(valuePos);
    }

    @Override
    public int op() {
        return SETINT;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setInt(block, offset, val, false);
        tx.unpin(block);
    }

    public String toString() {
        return "<SETINT " + txNum + " " + block + " " + offset + " " + val + ">";
    }

    public static int writeToLog(LogManager logManager, int txNum, BlockId block, int offset, int val) {
        int txPos = Integer.BYTES;
        int filePos = txPos + Integer.BYTES;
        int blockPos = filePos + Page.maxLength(block.fileName().length());
        int offsetPos = blockPos + Integer.BYTES;
        int valuePos = offsetPos + Integer.BYTES;
        byte[] record = new byte[valuePos + Integer.BYTES];
        Page page = new Page(record);
        page.setInt(0, SETINT);
        page.setInt(txPos, txNum);
        page.setString(filePos, block.fileName());
        page.setInt(blockPos, block.number());
        page.setInt(offsetPos, offset);
        page.setInt(valuePos, val);
        return logManager.append(record);
    }
}
