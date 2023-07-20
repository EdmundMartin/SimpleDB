package SimpleDB.transaction.recovery;

import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.transaction.Transaction;

public class RollbackRecord implements LogRecord {

    private final int txNum;

    public RollbackRecord(Page page) {
        int txPos = Integer.BYTES;
        txNum = page.getInt(txPos);
    }

    @Override
    public int op() {
        return ROLLBACK;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {}

    public String toString() { return "<ROLLBACK " + txNum + ">";}

    public static int writeToLog(LogManager logManager, int txNum) {
        byte[] record = new byte[2*Integer.BYTES];
        Page page = new Page(record);
        page.setInt(0, ROLLBACK);
        page.setInt(Integer.BYTES, txNum);
        return logManager.append(record);
    }
}
