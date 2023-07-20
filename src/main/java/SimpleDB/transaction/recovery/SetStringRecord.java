package SimpleDB.transaction.recovery;

import SimpleDB.file.BlockId;
import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.transaction.Transaction;

public class SetStringRecord implements LogRecord {
    private int txNum;
    private int offset;
    private String value;
    private BlockId block;

    public SetStringRecord(Page page) {
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
        value = page.getString(valuePos);
    }

    public int op() {
        return SETSTRING;
    }

    public int txNumber() {
        return txNum;
    }

    public String toString() {
        return "<SETSTRING " + txNum + " " + block + " " + offset + " " + value + ">";
    }

    public void undo(Transaction tx) {
        tx.pin(block);
        tx.setString(block, offset, value, false);
        tx.unpin(block);
    }

    public static int writeToLog(LogManager logManager, int txNum, BlockId block, int offset, String value) {
        int txPos = Integer.BYTES;
        int filePos = txPos + Integer.BYTES;
        int blockPos = filePos + Page.maxLength(block.fileName().length());
        int offsetPos = blockPos + Integer.BYTES;
        int valuePos = offsetPos + Integer.BYTES;
        int recordLength = valuePos + Page.maxLength(value.length());

        byte[] record = new byte[recordLength];
        Page p = new Page(record);
        p.setInt(0, SETSTRING);
        p.setInt(txPos, txNum);
        p.setString(filePos, block.fileName());
        p.setInt(blockPos, block.number());
        p.setInt(offsetPos, offset);
        p.setString(valuePos, value);
        return logManager.append(record);
    }
}
