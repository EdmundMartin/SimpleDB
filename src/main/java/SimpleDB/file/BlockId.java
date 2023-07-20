package SimpleDB.file;

import java.util.Objects;

public class BlockId {

    private final String filename;
    private final int blockNum;

    public BlockId(String filename, int blockNum) {
        this.filename = filename;
        this.blockNum = blockNum;
    }

    public String fileName() {
        return filename;
    }

    public int number() {
        return blockNum;
    }

    public boolean equals(Object obj) {
        if (getClass() != obj.getClass()) {
            return false;
        }
        BlockId blk = (BlockId) obj;
        return filename.equals(blk.filename) && blockNum == blk.blockNum;
    }

    public String toString() {
        return "[file " + filename + ", block " + blockNum + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, blockNum);
    }
}
