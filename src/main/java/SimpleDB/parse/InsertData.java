package SimpleDB.parse;

import SimpleDB.query.Constant;

import java.util.List;

public class InsertData {
    private final String tableName;
    private final List<String> fields;
    private final List<Constant> values;

    public InsertData(String tableName, List<String> fields, List<Constant> values) {
        this.tableName = tableName;
        this.fields = fields;
        this.values = values;
    }

    public String tableName() {
        return tableName;
    }

    public List<String> fields() {
        return fields;
    }

    public List<Constant> getValues() {
        return values;
    }
}
