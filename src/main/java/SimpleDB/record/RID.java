package SimpleDB.record;

/**
 * An identifier for a record within a file.
 * A RID consists of the block number in the file,
 * and the location of the record in that block
 */
public class RID {
    private int blockNum;
    private int slot;

    /**
     * Create a RID for the record having the specified
     * location in the specified block.
     * @param blockNum the block number where the record lives
     * @param slot the record's location
     */
    public RID(int blockNum, int slot) {
        this.blockNum = blockNum;
        this.slot = slot;
    }

    /**
     * Return the block number associated with this RID.
     * @return the block number
     */
    public int blockNumber() {
        return blockNum;
    }

    /**
     * Return the slot associated with this RID.
     * @return the slot
     */
    public int slot() {
        return slot;
    }

    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this.getClass() != o.getClass()) {
            return false;
        }
        RID rid = (RID) o;
        return blockNum == rid.blockNum && slot == rid.slot;
    }

    public String toString() {
        return "[ " + blockNum + ", " + slot + "]";
    }
}
