package SimpleDB.parse;

import SimpleDB.query.Predicate;

public class DeleteData {
    private final String tableName;
    private final Predicate predicate;

    public DeleteData(String tableName, Predicate predicate) {
        this.tableName = tableName;
        this.predicate = predicate;
    }

    public String tableName() {
        return tableName;
    }

    public Predicate predicate() {
        return predicate;
    }
}
