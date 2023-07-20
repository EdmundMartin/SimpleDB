package SimpleDB.transaction.recovery;

import SimpleDB.buffer.Buffer;
import SimpleDB.buffer.BufferManager;
import SimpleDB.file.BlockId;
import SimpleDB.log.LogManager;
import SimpleDB.transaction.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class RecoveryManager {
    private LogManager logManager;
    private BufferManager bufferManager;
    private Transaction tx;
    private int txNum;

    public RecoveryManager(Transaction tx, int txNum, LogManager logManager, BufferManager bufferManager) {
        this.tx = tx;
        this.txNum = txNum;
        this.logManager = logManager;
        this.bufferManager = bufferManager;
        StartRecord.writeToLog(logManager, txNum);
    }

    public void commit() {
        bufferManager.flushAll(txNum);
        int lsn = CommitRecord.writeToLog(logManager, txNum);
        logManager.flush(lsn);
    }

    /**
     * Write a rollback record to the log and flush it to disk
     */
    public void rollback() {
        doRollback();
        bufferManager.flushAll(txNum);
        int lsn = RollbackRecord.writeToLog(logManager, txNum);
        logManager.flush(lsn);
    }

    public void recover() {
        doRecover();
        bufferManager.flushAll(txNum);
        int lsn = CheckpointRecord.writeToLog(logManager);
        logManager.flush(lsn);
    }

    /**
     * Write a setint record to the log and return it's LSN.
     */
    public int setInt(Buffer buffer, int offset) {
        int oldVal = buffer.contents().getInt(offset);
        BlockId blockId = buffer.block();
        return SetIntRecord.writeToLog(logManager, txNum, blockId, offset, oldVal);
    }

    /**
     * Write a setstring record to the log and return its LSN.
     */
    public int setString(Buffer buffer, int offset) {
        String oldVal = buffer.contents().getString(offset);
        BlockId blk = buffer.block();
        return SetStringRecord.writeToLog(logManager, txNum, blk, offset, oldVal);
    }

    /**
     * Rollback the transaction, by iterating through the log records until it finds
     * the transactions START record, calling undo() for each of the transactions
     * log records
     */
    private void doRollback() {
        Iterator<byte[]> logIterator = logManager.iterator();
        while (logIterator.hasNext()) {
            byte[] bytes = logIterator.next();
            LogRecord record = LogRecord.createLogRecord(bytes);
            if (record.txNumber() == txNum) {
                if (record.op() == LogRecord.START) {
                    return;
                }
                record.undo(tx);
            }
        }
    }

    /**
     * Do a complete database recovery
     * The method iterates through the log records.
     * Whenever it finds a log record for an unfinished transaction,
     * it calls undo() on that record.
     * The method stops when it encounters a CHECKPOINT record
     * or the end of the log
     */
    private void doRecover() {
        Collection<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> logIterator = logManager.iterator();
        while (logIterator.hasNext()) {
            byte[] bytes = logIterator.next();
            LogRecord record = LogRecord.createLogRecord(bytes);
            int operation = record.op();
            if (operation == LogRecord.CHECKPOINT) {
                return;
            }
            if (operation == LogRecord.COMMIT || operation == LogRecord.ROLLBACK) {
                finishedTxs.add(record.txNumber());
            } else if (!finishedTxs.contains(record.txNumber())) {
                record.undo(tx);
            }
        }
    }
}
