package SimpleDB.transaction.recovery;

import SimpleDB.file.Page;
import SimpleDB.transaction.Transaction;

public interface LogRecord {

    // TODO - Refactor into an Enum
    static final int CHECKPOINT = 0, START = 1, COMMIT = 2, ROLLBACK = 3, SETINT = 4, SETSTRING = 5;

    /**
     * Returns the log record's type
     */
    int op();

    /**
     * Returns the transaction id stored with the log record
     * @return
     */
    int txNumber();

    /**
     * Undoes the operation encoded by this log record
     * The only log record types for which this method does anything interesting are
     * SETINT and SETSTRING
     */
    // TODO - Fix once transaction is implemented
    void undo(Transaction tx);


    static LogRecord createLogRecord(byte[] bytes) {
        Page p = new Page(bytes);
        switch (p.getInt(0)) {
            case CHECKPOINT:
                return new CheckpointRecord();
            case START:
                return new StartRecord(p);
            case COMMIT:
                return new CommitRecord(p);
            case ROLLBACK:
                return new RollbackRecord(p);
            case SETINT:
                // TODO - Implement rollback
                return new SetIntRecord(p);
            case SETSTRING:
                // TODO - Implement rollback
                return new SetStringRecord(p);
            default:
                return null;
        }
    }
}
