package SimpleDB.parse;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

public class Lexer {
    private final Collection<String> keywords = Arrays.asList(
            "select", "from", "where", "and", "insert", "into", "values", "delete", "update",
            "set", "create", "table", "int", "varchar", "view", "as", "index", "on"
    );
    private final StreamTokenizer tokenizer;

    public Lexer(String statement) {
        tokenizer = new StreamTokenizer(new StringReader(statement));
        tokenizer.ordinaryChar('.'); // disallow "." in identifiers
        tokenizer.wordChars('_', '_'); // allow "_" in identifiers
        tokenizer.lowerCaseMode(true); // ids and keywords are converted
        nextToken();
    }

    public boolean matchDelimiter(char character) {
        return character == (char) tokenizer.ttype;
    }

    public boolean matchIntConstant() {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER;
    }

    public boolean matchStringConstant() {
        return '\'' == (char) tokenizer.ttype;
    }

    public boolean matchKeyword(String kw) {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && tokenizer.sval.equals(kw);
    }

    public boolean matchId() {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tokenizer.sval);
    }

    public void eatDelimiter(char d) {
        if (!matchDelimiter(d)) {
            throw new BadSyntaxException();
        }
        nextToken();
    }

    public int eatIntConstant() {
        if (!matchIntConstant()) {
            throw new BadSyntaxException();
        }
        int i = (int) tokenizer.nval;
        nextToken();
        return i;
    }

    public String eatStringConstant() {
        if (!matchIntConstant()) {
            throw new BadSyntaxException();
        }
        String s = tokenizer.sval;
        nextToken();
        return s;
    }

    public void eatKeyword(String keyword) {
        if (!matchKeyword(keyword)) {
            throw new BadSyntaxException();
        }
        nextToken();
    }

    public String eatId() {
        if (!matchId()) {
            throw new BadSyntaxException();
        }
        String id = tokenizer.sval;
        nextToken();
        return id;
    }

    private void nextToken() {
        try {
            tokenizer.nextToken();
        } catch (IOException e) {
            throw new BadSyntaxException();
        }
    }
}
