package SimpleDB.metadata;

import SimpleDB.record.Layout;
import SimpleDB.record.Schema;
import SimpleDB.record.TableScan;
import SimpleDB.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class TableManager {

    public static final int MAX_NAME_LENGTH = 16;
    private final Layout tableCatalogue;
    private final Layout fieldCatalogue;

    private static final String TABLE_NAME = "tblname";
    private static final String SLOT_SIZE = "slotsize";
    private static final String FIELD_NAME = "fieldname";
    private static final String TYPE = "type";
    private static final String LENGTH = "length";
    private static final String OFFSET = "offset";

    private static final String TABLE_CATALOGUE_NAME = "tblcat";
    private static final String FIELD_CATALOGUE_NAME = "fldcat";

    public TableManager(boolean isNew, Transaction tx) {
        Schema tableCatSchema = new Schema();
        tableCatSchema.addStringField(TABLE_NAME, MAX_NAME_LENGTH);
        tableCatSchema.addIntField(SLOT_SIZE);
        tableCatalogue = new Layout(tableCatSchema);

        Schema fieldCatSchema = new Schema();
        fieldCatSchema.addStringField(TABLE_NAME, MAX_NAME_LENGTH);
        fieldCatSchema.addStringField(FIELD_NAME, MAX_NAME_LENGTH);
        fieldCatSchema.addIntField(TYPE);
        fieldCatSchema.addIntField(LENGTH);
        fieldCatSchema.addIntField(OFFSET);

        fieldCatalogue = new Layout(fieldCatSchema);

        if (isNew) {
            createTable(TABLE_CATALOGUE_NAME, tableCatSchema, tx);
            createTable(FIELD_CATALOGUE_NAME, fieldCatSchema, tx);
        }
    }

    public void createTable(String tableName, Schema schema, Transaction tx) {
        Layout layout = new Layout(schema);

        TableScan tableCat = new TableScan(tx, TABLE_CATALOGUE_NAME, tableCatalogue);
        tableCat.insert();
        tableCat.setString(TABLE_NAME, tableName);
        tableCat.setInt(SLOT_SIZE, layout.slotSize());
        tableCat.close();

        TableScan fieldCat = new TableScan(tx, FIELD_CATALOGUE_NAME, fieldCatalogue);
        for (String fieldName: schema.fields()) {
            fieldCat.insert();
            fieldCat.setString(TABLE_NAME, tableName);
            fieldCat.setString(FIELD_NAME, fieldName);
            fieldCat.setInt(TYPE, schema.type(fieldName));
            fieldCat.setInt(LENGTH, schema.length(fieldName));
            fieldCat.setInt(OFFSET, layout.offset(fieldName));
        }
        fieldCat.close();
    }

    public Layout getLayout(String tableName, Transaction tx) {
        int size = -1;
        TableScan tableCat = new TableScan(tx, TABLE_CATALOGUE_NAME, tableCatalogue);

        while (tableCat.next()) {
            if (tableCat.getString(tableName).equals(tableName)) {
                size = tableCat.getInt(SLOT_SIZE);
                break;
            }
        }
        tableCat.close();

        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<>();
        TableScan fieldCat = new TableScan(tx, FIELD_CATALOGUE_NAME, fieldCatalogue);

        while(fieldCat.next()) {
            if (fieldCat.getString(TABLE_NAME).equals(tableName)) {
                String fieldName = fieldCat.getString(FIELD_NAME);
                int fieldType = fieldCat.getInt(TYPE);
                int fieldLen = fieldCat.getInt(LENGTH);
                int offset = fieldCat.getInt(OFFSET);
                offsets.put(fieldName, offset);
                schema.addField(fieldName, fieldType, fieldLen);
            }
        }
        fieldCat.close();

        return new Layout(schema, offsets, size);
    }
}
