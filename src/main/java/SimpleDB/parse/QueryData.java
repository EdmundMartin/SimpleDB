package SimpleDB.parse;

import SimpleDB.query.Predicate;

import java.util.Collection;
import java.util.List;

public class QueryData {
    private final List<String> fields;
    private final Collection<String> tables;
    private final Predicate predicate;


    public QueryData(List<String> fields, Collection<String> tables, Predicate predicate) {
        this.fields = fields;
        this.tables = tables;
        this.predicate = predicate;
    }

    public List<String> fields() {
        return this.fields;
    }

    public Collection<String> tables() {
        return this.tables;
    }

    public Predicate predicate() {
        return this.predicate;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("select ");
        for (String fieldName: fields) {
            builder.append(fieldName);
            builder.append(", ");
        }
        builder.replace(builder.length()-2, builder.length(), ""); // remove final comma
        builder.append(" from ");

        for (String tableName: tables) {
            builder.append(tableName);
            builder.append(", ");
        }
        builder.replace(builder.length()-2, builder.length(), ""); // remove final comma
        String predicateString = predicate.toString();
        if (!predicateString.equals("")) {
            builder.append(" where ");
            builder.append(predicateString);
        }
        return builder.toString();
    }
}
