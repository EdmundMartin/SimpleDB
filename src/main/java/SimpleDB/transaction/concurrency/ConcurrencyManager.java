package SimpleDB.transaction.concurrency;

import SimpleDB.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {

    private static final LockTable lockTable = new LockTable();
    private final Map<BlockId, String> locks = new HashMap<>();

    public void sLock(BlockId blockId) {
        if (locks.get(blockId) == null) {
            lockTable.sLock(blockId);
            locks.put(blockId, "S");
        }
    }

    public void xLock(BlockId blockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId);
            lockTable.xLock(blockId);
            locks.put(blockId, "X");
        }
    }

    public void release() {
        locks.keySet().forEach(lockTable::unlock);
        locks.clear();
    }

    private boolean hasXLock(BlockId blockId) {
        String lockType = locks.get(blockId);
        return lockType != null && lockType.equals("X");
    }
}
