package SimpleDB.transaction;

import SimpleDB.buffer.Buffer;
import SimpleDB.buffer.BufferManager;
import SimpleDB.file.BlockId;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;
import SimpleDB.log.LogManager;
import SimpleDB.transaction.concurrency.ConcurrencyManager;

public class Transaction {
    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
    // TODO - Recovery Manager
    private final ConcurrencyManager concurrencyManager;
    private final BufferManager bufferManager;
    private final FileManager fileManager;
    private int txNum;
    private final BufferList buffers;


    /**
     * Create a new transaction and its associated
     * recovery and concurrency managers
     */
    public Transaction(FileManager fileManager, LogManager logManager, BufferManager bufferManager) {
        this.fileManager = fileManager;
        this.bufferManager = bufferManager;
        txNum = nextTxNumber();
        // TODO - Recovery and Concurrency
        concurrencyManager = new ConcurrencyManager();
        buffers = new BufferList(this.bufferManager);
    }

    /**
     * Commit the current transaction.
     * Flush all modified buffers (and their log records),
     * write and flush a commit record to the log,
     * release all locks, and unpin any pinned buffers.
     */
    public void commit() {
        // recovery commit
        System.out.println("transaction " + txNum + " committed");
        concurrencyManager.release();
        buffers.unpinAll();
    }

    /**
     * Rollback the current transaction.
     * Undo any modified values,
     * flush those buffers,
     * write and flush a rollback to the log,
     * release all locks, and unpin any pinned buffers.
     */
    public void rollback() {
        // recovery rollback
        System.out.println("transaction " + txNum + " rolled back");
        concurrencyManager.release();
        buffers.unpinAll();
    }

    /**
     * Flush all modified buffers.
     * Then go through the log, rolling back all uncommitted transactions.
     * Finally, write a quiescent checkpoint record to the log. This method is called
     * during system startup, before user transactions begin.
     */
    public void recover() {
        bufferManager.flushAll(txNum);
        // recoveryManager.recover();
    }

    public void pin(BlockId block) {
        buffers.pin(block);
    }

    public void unpin(BlockId block) {
        buffers.unpin(block);
    }

    /**
     * Return the integer value stored at the specific offset
     * of the specified block.
     * The method first obtains a lock on the block and then it calls
     * the buffer to retrieve the value
     */
    public int getInt(BlockId block, int offset) {
        concurrencyManager.sLock(block);
        Buffer buffer = buffers.getBuffer(block);
        return buffer.contents().getInt(offset);
    }

    /**
     * Return the string value stored at the specific offset
     * of the specified block.
     * The method first obtains a lock on the block and then it calls
     * the buffer to retrieve the value
     */
    public String getString(BlockId block, int offset) {
        concurrencyManager.sLock(block);
        Buffer buffer = buffers.getBuffer(block);
        return buffer.contents().getString(offset);
    }

    public void setInt(BlockId blockId, int offset, int value, boolean okToLog) {
        concurrencyManager.xLock(blockId);
        Buffer buffer = buffers.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            // recovery manager setInt
        }
        Page page = buffer.contents();
        page.setInt(offset, value);
        buffer.setModified(txNum, lsn);
    }

    public void setString(BlockId block, int offset, String value, boolean okToLog) {
        concurrencyManager.xLock(block);
        Buffer buffer = buffers.getBuffer(block);
        int lsn = -1;
        if (okToLog) {
            // recovery manager - set string TODO
        }
        Page page = buffer.contents();
        page.setString(offset, value);
        buffer.setModified(txNum, lsn);
    }

    /**
     * Return the number of blocks in the specified file.
     * This method first obtains an slock on the EOF, before
     * asking the file manager to return the file size.
     */
    public int size(String filename) {
        BlockId dummyBlock = new BlockId(filename, END_OF_FILE);
        concurrencyManager.sLock(dummyBlock);
        return fileManager.length(filename);
    }

    /**
     * Append a new block to the end of the specified file
     * and returns a reference to it.
     * This method first obtains an XLock on the EOF
     * before performing the append.
     */
    public BlockId append(String filename) {
        BlockId dummyBlock = new BlockId(filename, END_OF_FILE);
        concurrencyManager.xLock(dummyBlock);
        return fileManager.append(filename);
    }

    public int blockSize() {
        return fileManager.blockSize();
    }

    public int availableBuffers() {
        return bufferManager.available();
    }

    public static synchronized int nextTxNumber() {
        nextTxNum++;
        return nextTxNum;
    }

}
