package SimpleDB.metadata;

/**
 * A StatInfo object holds three pieces of statistical information about a table:
 * the number of blocks, the number of records,
 * and the number of distinct values for each field.
 */
public class StatInfo {
    private final int numBlocks;
    private final int numRecords;

    /**
     * Create a StatInfo object.
     * Note that the number of distinct values is not passed into the constructor.
     * The object fakes this value.
     */
    public StatInfo(int numBlocks, int numRecords) {
        this.numBlocks = numBlocks;
        this.numRecords = numRecords;
    }

    public int blocksAccessed() {
        return numBlocks;
    }

    public int recordsOutput() {
        return numRecords;
    }

    /**
     * Estimate is complete guess because doing something reasonable is beyond scope of
     * this system.
     */
    public int distinctValues(String fieldName) {
        return 1 + (numRecords / 3);
    }
}
