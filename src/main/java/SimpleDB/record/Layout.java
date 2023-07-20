package SimpleDB.record;

import SimpleDB.file.Page;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;

public class Layout {
    private final Schema schema;
    private final Map<String, Integer> offsets;
    private final int slotSize;


    public Layout(Schema schema) {
        this.schema = schema;
        offsets = new HashMap<>();
        int pos = Integer.BYTES; // leave space for the empty/inuse flag
        for (String fieldName: schema.fields()) {
            offsets.put(fieldName, pos);
            pos += lengthInBytes(fieldName);
        }
        slotSize = pos;
    }

    public Layout(Schema schema, Map<String,Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    public Schema schema() {
        return schema;
    }

    public int offset(String fieldName) {
        return offsets.get(fieldName);
    }

    public int slotSize() {
        return slotSize;
    }

    private int lengthInBytes(String fieldName) {
        int fieldType = schema.type(fieldName);
        if (fieldType == INTEGER) {
            return Integer.BYTES;
        }
        return Page.maxLength(schema.length(fieldName));
    }
}
