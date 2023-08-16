package SimpleDB.parse;

import SimpleDB.query.Expression;
import SimpleDB.query.Predicate;

public class ModifyData {
    private final String tableName;
    private final String fieldName;
    private final Expression newValue;
    private final Predicate predicate;

    public ModifyData(String tableName, String fieldName, Expression newValue, Predicate predicate) {
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.newValue = newValue;
        this.predicate = predicate;
    }

    public String tableName() {
        return tableName;
    }

    public String targetField() {
        return fieldName;
    }

    public Expression newValue() {
        return newValue;
    }

    public Predicate predicate() {
        return predicate;
    }
}
