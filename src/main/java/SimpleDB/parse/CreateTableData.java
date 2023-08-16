package SimpleDB.parse;

import SimpleDB.record.Schema;

public class CreateTableData {
    private final String tableName;
    private final Schema schema;

    public CreateTableData(String tableName, Schema schema) {
        this.tableName = tableName;
        this.schema = schema;
    }

    public String tableName() {
        return tableName;
    }

    public Schema newSchema() {
        return schema;
    }
}
