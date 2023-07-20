package SimpleDB.transaction;

import SimpleDB.buffer.Buffer;
import SimpleDB.buffer.BufferManager;
import SimpleDB.file.BlockId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferList {
    private final Map<BlockId, Buffer> buffers = new HashMap<>();
    private final List<BlockId> pins = new ArrayList<>();
    private final BufferManager bufferManager;

    public BufferList(BufferManager manager) {
        this.bufferManager = manager;
    }

    Buffer getBuffer(BlockId block) {
        return buffers.get(block);
    }

    void pin(BlockId blockId) {
        Buffer buffer = bufferManager.pin(blockId);
        buffers.put(blockId, buffer);
        pins.add(blockId);
    }

    void unpin(BlockId blockId) {
        Buffer buffer = buffers.get(blockId);
        bufferManager.unpin(buffer);
        pins.remove(blockId);
        if (!pins.contains(blockId)) {
            buffers.remove(blockId);
        }
    }

    void unpinAll() {
        pins.forEach(blk -> {
            Buffer buffer = buffers.get(blk);
            bufferManager.unpin(buffer);
        });
        buffers.clear();
        pins.clear();
    }
}
