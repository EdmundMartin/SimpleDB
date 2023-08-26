package SimpleDB.query;

import SimpleDB.plan.Plan;
import SimpleDB.record.Schema;

/**
 * A term is a comparison between two expressions.
 */
public class Term {
    private Expression leftExpr;
    private Expression rightExpr;

    /**
     * Create a new term that compares two expressions for equality
     */
    public Term(Expression leftExpr, Expression rightExpr) {
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
    }

    /**
     * Return true if both of the term's expressions
     * evaluate to the same constant with respect to the specified sca.
     */
    public boolean isSatisfied(Scan scan) {
        Constant left = leftExpr.evaluate(scan);
        Constant right = rightExpr.evaluate(scan);
        return right.equals(left);
    }

    // TODO - Reduction factor
    public int reductionFactor(Plan plan) {
        String leftName;
        String rightName;

        if (leftExpr.isFieldName() && rightExpr.isFieldName()) {
            leftName = leftExpr.asFieldName();
            rightName = rightExpr.asFieldName();
            return Math.max(plan.distinctValues(leftName), plan.distinctValues(rightName));
        }

        if (leftExpr.isFieldName()) {
            leftName = leftExpr.asFieldName();
            return plan.distinctValues(leftName);
        }

        if (rightExpr.isFieldName()) {
            rightName = rightExpr.asFieldName();
            return plan.distinctValues(rightName);
        }

        if (leftExpr.asConstant().equals(rightExpr.asConstant())) {
            return 1;
        }

        return Integer.MAX_VALUE;
    }

    /**
     * Determine if this term is of the form "F=C"
     * where F is the specified field and C is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null
     */
    public Constant equatesWithConstant(String fieldName) {
        if ((leftExpr.isFieldName() && leftExpr.asFieldName().equals(fieldName)) && !rightExpr.isFieldName()) {
            return rightExpr.asConstant();
        } else if (rightExpr.isFieldName() && rightExpr.asFieldName().equals(fieldName) && !leftExpr.isFieldName()) {
            return leftExpr.asConstant();
        }
        return null;
    }

    public String equatesWithField(String fieldName) {
        if ((leftExpr.isFieldName() && leftExpr.asFieldName().equals(fieldName)) && rightExpr.isFieldName()) {
            return rightExpr.asFieldName();
        } else if ((rightExpr.isFieldName() && rightExpr.asFieldName().equals(fieldName)) && leftExpr.isFieldName()) {
            return leftExpr.asFieldName();
        }
        return null;
    }

    public boolean appliesTo(Schema schema) {
        return leftExpr.appliesTo(schema) && rightExpr.appliesTo(schema);
    }

    public String toString() {
        return leftExpr.toString() + " = " + rightExpr.toString();
    }
}
