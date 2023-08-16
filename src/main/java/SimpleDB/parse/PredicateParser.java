package SimpleDB.parse;

public class PredicateParser {
    private Lexer lexer;

    public PredicateParser(String stmt) {
        this.lexer = new Lexer(stmt);
    }

    public String field() {
        return lexer.eatId();
    }

    public void constant() {
        if (lexer.matchStringConstant()) {
            lexer.eatStringConstant();
        } else {
            lexer.eatIntConstant();
        }
    }

    public void expression() {

    }

    public void term() {

    }
}
