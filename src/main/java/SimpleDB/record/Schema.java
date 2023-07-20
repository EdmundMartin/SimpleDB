package SimpleDB.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * The record schema of a table.
 * A schema contains the name and type of
 * each field of the table, as well as the length
 * of each varchar field
 */
public class Schema {
    private final List<String> fields = new ArrayList<>();
    private final Map<String, FieldInfo> info = new HashMap<>();

    /**
     * Add a field to the schema having a specified name, type and length.
     * If the field type is 'integer', then the length value is irrelevant.
     */
    public void addField(String fieldName, int type, int length) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(type, length));
    }

    /**
     * Add an integer field to the schema.
     */
    public void addIntField(String fieldName) {
        addField(fieldName, INTEGER, 0);
    }

    /**
     * Add a string field to the schema.
     * The length is the conceptual length of the field.
     * For example, if the field is defined as varchar(8)
     * the it's length is 8.
     */
    public void addStringField(String fieldName, int length) {
        addField(fieldName, VARCHAR, length);
    }

    /**
     * Add a field to the schema having the same type and length as the corresponding field
     * in another schema.
     */
    public void add(String fieldName, Schema schema) {
        int type = schema.type(fieldName);
        int length = schema.length(fieldName);
        addField(fieldName, type, length);
    }

    /**
     * Add all of the fields in the specified schema to current schema.
     */
    public void addAll(Schema schema) {
        schema.fields.forEach(field -> {
            add(field, schema);
        });
    }

    public List<String> fields() {
        return fields;
    }

    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    /**
     * Return the type of the specified field using the
     * constants in {@link java.sql.Types}
     */
    public int type(String fieldName) {
        return info.get(fieldName).type;
    }

    /**
     * Return th conceptual length of the specified field.
     * If the field is not a string field, then the return value is
     * undefined.
     */
    public int length(String fieldName) {
        return info.get(fieldName).length;
    }

    class FieldInfo {
        int type;
        int length;

        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }
}
