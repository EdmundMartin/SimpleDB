package SimpleDB.buffer;

import SimpleDB.file.BlockId;
import SimpleDB.file.FileManager;
import SimpleDB.log.LogManager;

import java.util.Arrays;
import java.util.Optional;

public class BufferManager {
    private Buffer[] bufferPool;
    private int numAvailable;
    private static final long MAX_TIME = 10_000;

    /**
     * Creates a buffer manager having the specified number
     * of buffer slots.
     */
    public BufferManager(FileManager fileManager, LogManager logManager, int numberOfSlots) {
        bufferPool = new Buffer[numberOfSlots];
        numAvailable = numberOfSlots;

        for (int i = 0; i < numberOfSlots; i++) {
            bufferPool[i] = new Buffer(fileManager, logManager);
        }
    }

    public synchronized int available() {
        return numAvailable;
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     */
    public synchronized void flushAll(int txNum) {
        // TODO - Refactor for Java8 niceness
        for (Buffer buff: bufferPool) {
            if (buff.modifyingTxNumber() == txNum) {
                buff.flush();
            }
        }
    }

    /**
     * Unpins the specified data buffer.
     * If the pin count goes to zero then notify any awaiting threads
     */
    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    public synchronized Buffer pin(BlockId blockId) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = tryToPin(blockId);

            while (buffer == null && !waitingTimeTooLong(timestamp)) {
                wait(MAX_TIME);
                buffer = tryToPin(blockId);
            }
            if (buffer == null) {
                throw new BufferAbortionException();
            }
            return buffer;
        } catch (InterruptedException e) {
            throw new BufferAbortionException();
        }
    }

    private boolean waitingTimeTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }


    private Buffer tryToPin(BlockId blockId) {
        Buffer buffer = findExistingBuffer(blockId);
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer();
            if (buffer == null) {
                return null;
            }
            buffer.assignToBlock(blockId);
        }
        if (!buffer.isPinned()) {
            numAvailable--;
        }
        buffer.pin();
        return buffer;
    }

    private Buffer findExistingBuffer(BlockId blockId) {
        for (Buffer buffer: bufferPool) {
            BlockId block = buffer.block();
            if (block != null && block.equals(blockId)) {
                return buffer;
            }
        }
        return null;
    }

    private Buffer chooseUnpinnedBuffer() {
        Optional<Buffer> picked = Arrays.stream(bufferPool).filter(buffer -> !buffer.isPinned()).findFirst();
        return picked.orElse(null);
    }
}
