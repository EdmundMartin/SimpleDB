package SimpleDB.parse;

public class CreateIndexData {
    private final String indexName;
    private final String tableName;
    private final String fieldName;

    public CreateIndexData(String indexName, String tableName, String fieldName) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.fieldName = fieldName;
    }

    public String indexName() {
        return indexName;
    }

    public String tableName() {
        return tableName;
    }

    public String fieldName() {
        return fieldName;
    }

}
