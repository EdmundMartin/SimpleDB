package SimpleDB.query;

import SimpleDB.record.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Predicate {
    private final List<Term> terms = new ArrayList<>();

    /**
     * Create an empty predicate, corresponding to 'true'
     */
    public Predicate() {}

    /**
     * Create a predicate containing a single term.
     * @param term
     */
    public Predicate(Term term) {
        terms.add(term);
    }

    /**
     * Modifies the predicate to be the conjunction of itself and the specified predicate.
     */
    public void conjoinWith(Predicate predicate) {
        terms.addAll(predicate.terms);
    }

    /**
     * Returns true if the predicate evaluates to true with respect to the specified scan.
     */
    public boolean isSatisfied(Scan scan) {
        for (Term t: terms) {
            if (!t.isSatisfied(scan)) {
                return false;
            }
        }
        return true;
    }

    // TODO - Reduction factor

    /**
     * Return the sub-predicate that applies to the specified schema.
     */
    public Predicate selectSubPredicate(Schema schema) {
        Predicate result = new Predicate();
        for (Term t: terms) {
            if (t.appliesTo(schema)) {
                result.terms.add(t);
            }
        }
        if (result.terms.size() == 0) {
            return null;
        }
        return result;
    }

    /**
     * Return the sub-predicate consisting of terms that apply, to the union of the two specified schemas,
     * but not to either schema separately.
     */
    public Predicate joinSubPredicate(Schema first, Schema second) {
        Predicate result = new Predicate();
        Schema newSchema = new Schema();
        newSchema.addAll(first);
        newSchema.addAll(second);

        for (Term t: terms) {
            if (!t.appliesTo(first) && !t.appliesTo(second) && t.appliesTo(newSchema)) {
                result.terms.add(t);
            }
        }

        if (result.terms.size() == 0) {
            return null;
        }

        return result;
    }

    @Override
    public String toString() {
        Iterator<Term> iterator = terms.iterator();
        if (!iterator.hasNext()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        builder.append(iterator.next().toString());
        while (iterator.hasNext()) {
            builder.append(" and ");
            builder.append(iterator.next().toString());
        }
        return builder.toString();
    }
}
