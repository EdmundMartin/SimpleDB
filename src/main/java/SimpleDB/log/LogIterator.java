package SimpleDB.log;

import SimpleDB.file.BlockId;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;

import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {

    private FileManager fileManager;
    private BlockId blockId;
    private Page page;
    private int currentPosition;
    private int boundary;

    public LogIterator(FileManager fileManager, BlockId blockId) {
        this.fileManager = fileManager;
        this.blockId = blockId;
        byte[] bytes = new byte[fileManager.blockSize()];
        this.page = new Page(bytes);
        moveToBlock(blockId);
    }


    @Override
    public boolean hasNext() {
        return currentPosition < fileManager.blockSize() || blockId.number() > 0;
    }

    /**
     * Moves to the next log record in the block.
     * If there are no more log records in the block,
     * then move to the previous block
     * and return the log record from there.
     * @return the next earliest log record
     */
    @Override
    public byte[] next() {
        if (currentPosition == fileManager.blockSize()) {
            blockId = new BlockId(blockId.fileName(), blockId.number()-1);
            moveToBlock(blockId);
        }
        byte[] record = page.getBytes(currentPosition);
        currentPosition += Integer.BYTES + record.length;
        return record;
    }

    /**
     * Moves to the specified log block
     * and positions it at the first record in the block
     * (i.e, the most recent one).
     */
    private void moveToBlock(BlockId blockId) {
        fileManager.read(blockId, page);
        boundary = page.getInt(0);
        currentPosition = boundary;
    }
}
