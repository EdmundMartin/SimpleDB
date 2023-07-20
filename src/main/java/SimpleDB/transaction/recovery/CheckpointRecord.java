package SimpleDB.transaction.recovery;

import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.transaction.Transaction;

public class CheckpointRecord implements LogRecord {

    public CheckpointRecord() {}

    @Override
    public int op() {
        return CHECKPOINT;
    }

    @Override
    public int txNumber() {
        return -1;
    }

    @Override
    public void undo(Transaction tx) {}

    @Override
    public String toString() { return "<CHECKPOINT>";}

    public static int writeToLog(LogManager logManager) {
        byte[] record = new byte[Integer.BYTES];
        Page p = new Page(record);
        p.setInt(0, CHECKPOINT);
        return logManager.append(record);
    }
}
