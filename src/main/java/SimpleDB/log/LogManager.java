package SimpleDB.log;


import SimpleDB.file.BlockId;
import SimpleDB.file.FileManager;
import SimpleDB.file.Page;

import java.util.Iterator;

public class LogManager {

    private final FileManager fileManager;
    private final String logFile;
    private final Page logPage;
    private BlockId currentBlock;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public LogManager(FileManager fileManager, String logFile) {
        this.fileManager = fileManager;
        this.logFile = logFile;
        byte[] bytes = new byte[fileManager.blockSize()];
        logPage = new Page(bytes);

        int logSize = fileManager.length(logFile);
        if (logSize == 0) {
            currentBlock = appendNewBlock();
        } else {
            currentBlock = new BlockId(logFile, logSize-1);
            fileManager.read(currentBlock, logPage);
        }
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    private void flush() {
        fileManager.write(currentBlock, logPage);
        lastSavedLSN = latestLSN;
    }

    public synchronized int append(byte[] logRecord) {
        int boundary = logPage.getInt(0);
        int recordSize = logRecord.length;
        int bytesNeeded = recordSize + Integer.BYTES;

        if (boundary - bytesNeeded < Integer.BYTES) { // Log record doesn't fit
            flush();
            currentBlock = appendNewBlock();
            boundary = logPage.getInt(0);
        }
        int recordPosition = boundary - bytesNeeded;

        logPage.setBytes(recordPosition, logRecord);
        logPage.setInt(0, recordPosition);
        latestLSN += 1;
        return latestLSN;
    }

    private BlockId appendNewBlock() {
        BlockId blk = fileManager.append(logFile);
        logPage.setInt(0, fileManager.blockSize());
        fileManager.write(blk, logPage);
        return blk;
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileManager, currentBlock);
    }
}
