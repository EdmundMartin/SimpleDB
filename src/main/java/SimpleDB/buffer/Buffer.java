package SimpleDB.buffer;


import SimpleDB.file.BlockId;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;
import SimpleDB.log.LogManager;

/**
 * An individual buffer. A databuffer wraps a page
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified
 * and if so, the id and lsn of the modifying transaction
 */
public class Buffer {
    private FileManager fileManager;
    private LogManager logManager;
    private Page contents;
    private BlockId blockId = null;
    private int pins = 0;
    private int txNum = -1;
    private int lsn = -1;


    public Buffer(FileManager fileManager, LogManager logManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        contents = new Page(fileManager.blockSize());
    }

    public Page contents() {
        return contents;
    }

    /**
     * Returns a reference to the disk block
     * allocated to the buffer
     */
    public BlockId block() {
        return blockId;
    }

    public void setModified(int txNum, int lsn) {
        this.txNum = txNum;
        if (lsn >= 0) {
            this.lsn = lsn;
        }
    }

    /**
     * Return true if the buffer is currently pinned
     * This means has a nonzero pin count
     */
    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTxNumber() {
        return txNum;
    }

    /**
     * Reads the contents of the specified block into
     * the contents of the buffer
     * If the buffer was dirty, then its previous contents
     * are first written to disk
     */
    void assignToBlock(BlockId blockId) {
        flush();
        this.blockId = blockId;
        fileManager.read(blockId, contents);
        pins = 0;
    }

    /**
     * Write the buffer to its disk block if it is dirty.
     */
    void flush() {
        if (txNum >= 0) {
            logManager.flush(lsn);
            fileManager.write(blockId, contents);
            txNum = -1;
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }

}
