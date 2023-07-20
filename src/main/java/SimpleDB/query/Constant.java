package SimpleDB.query;

public class Constant implements Comparable<Constant> {
    private Integer iVal = null;
    private String sVal = null;

    public Constant(Integer iVal) {
        this.iVal = iVal;
    }

    public Constant(String sVal) {
        this.sVal = sVal;
    }

    public int asInt() {
        return iVal;
    }

    public String asString() {
        return sVal;
    }

    @Override
    public boolean equals(Object object) {
        if (getClass() != object.getClass()) {
            return false;
        }
        Constant other = (Constant) object;
        return (iVal != null) ? iVal.equals(other.iVal) : sVal.equals(other.sVal);
    }


    @Override
    public int compareTo(Constant o) {
        return (iVal != null) ? iVal.compareTo(o.iVal) : sVal.compareTo(o.sVal);
    }

    @Override
    public int hashCode() {
        return (iVal != null) ? iVal.hashCode() : sVal.hashCode();
    }

    @Override
    public String toString() {
        return (iVal != null) ? iVal.toString() : sVal;
    }
}
