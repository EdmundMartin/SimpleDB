package SimpleDB.query;

import SimpleDB.record.Schema;

public class Expression {
    private Constant value = null;
    private String fieldName = null;

    public Expression(Constant value) {
        this.value = value;
    }

    public Expression(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Evaluate the expression with respect to the current record of the specified span
     */
    public Constant evaluate(Scan scan) {
        return value != null ? value : scan.getVal(fieldName);
    }

    /**
     * Return true if the expression is a field reference
     */
    public boolean isFieldName() {
        return fieldName != null;
    }

    /**
     * Return the constant corresponding to a constant expression or null if the expression does not denote
     * a constant.
     */
    public Constant asConstant() {
        return value;
    }

    /**
     * Return the field name corresponding to a constant expression or null if the expression does not denote a field.
     */
    public String asFieldName() {
        return fieldName;
    }

    /**
     * Determine if all of the fields mentioned in this expression are contained in the specified schema.
     */
    public boolean appliesTo(Schema schema) {
        return value != null || schema.hasField(fieldName);
    }

    @Override
    public String toString() {
        return value != null ? value.toString() : fieldName;
    }
}
