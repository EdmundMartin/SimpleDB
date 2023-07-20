package SimpleDB.transaction.concurrency;

import SimpleDB.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    private static final long MAX_TIME = 10_000; // 10 seconds

    private final Map<BlockId, Integer> locks = new HashMap<>();

    /**
     * Grant an SLock on the specified block.
     * If an XLock exists when the method is called
     * then the calling thread will be places on a wait list
     * until the lock is released.
     * If the thread remains on the wait list for a certain
     * amount of time an exception is thrown.
     */
    public synchronized void sLock(BlockId block) {
        try {
            long timestamp = System.currentTimeMillis();

            while (hasXLock(block) && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
            }
            if (hasXLock(block)) {
                throw new LockAbortException();
            }
            int val = getLockValue(block);
            locks.put(block, val+1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * Grant an XLock on the specified block.
     * If a lock of any type exists when the method is called
     * then the calling thread will be placed on a wait list
     * until the locks are released.
     * If the thread remains on the wait list for a certain amount of time
     * an exception is thrown.
     */
    synchronized void xLock(BlockId block) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSLocks(block) && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
            }
            if (hasOtherSLocks(block)) {
                throw new LockAbortException();
            }
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * Release a lock on the specified block
     * If this lock is the last lock on that block
     * then the waiting transactions are notified.
     */
    synchronized void unlock(BlockId block) {
        int val = getLockValue(block);
        if (val > 1) {
            locks.put(block, val-1);
        } else {
            locks.remove(block);
            notifyAll();
        }
    }

    private boolean hasXLock(BlockId block) {
        return getLockValue(block) < 0;
    }

    private boolean hasOtherSLocks(BlockId block) {
        return getLockValue(block) > 1;
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }

    private int getLockValue(BlockId block) {
        Integer integerVal = locks.get(block);
        return (integerVal == null) ? 0 : integerVal;
    }
}
