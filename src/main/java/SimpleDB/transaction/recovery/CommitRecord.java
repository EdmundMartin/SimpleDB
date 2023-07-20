package SimpleDB.transaction.recovery;

import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.transaction.Transaction;

public class CommitRecord implements LogRecord {

    private int txNum;

    public CommitRecord(Page page) {
        int txPos = Integer.BYTES;
        txNum = page.getInt(txPos);
    }


    @Override
    public int op() {
        return COMMIT;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {}


    public String toString() { return "<COMMIT " + txNum + ">";}

    /**
     * A static method to write a commit record to the log
     * This log record contains the COMMIT operator
     * followed by the transaction id.
     */
    public static int writeToLog(LogManager logManager, int txNum) {
        byte[] record = new byte[2*Integer.BYTES];
        Page page = new Page(record);
        page.setInt(0, COMMIT);
        page.setInt(Integer.BYTES, txNum);
        return logManager.append(record);
    }
}
