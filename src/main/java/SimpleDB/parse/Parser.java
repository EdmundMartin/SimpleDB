package SimpleDB.parse;

import SimpleDB.query.Constant;
import SimpleDB.query.Expression;
import SimpleDB.query.Predicate;
import SimpleDB.query.Term;
import SimpleDB.record.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Parser {
    private Lexer lexer;

    public Parser(String stmt) {
        this.lexer = new Lexer(stmt);
    }

    public String field() {
        return lexer.eatId();
    }

    public Constant constant() {
        if (lexer.matchStringConstant()) {
            return new Constant(lexer.eatStringConstant());
        }
        return new Constant(lexer.eatIntConstant());
    }

    public Expression expression() {
        if (lexer.matchId()) {
            return new Expression(field());
        }
        return new Expression(constant());
    }

    public Term term() {
        Expression left = expression();
        lexer.eatDelimiter('=');
        Expression right = expression();
        return new Term(left, right);
    }

    public Predicate predicate() {
        Predicate predicate = new Predicate(term());
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and");
            predicate.conjoinWith(predicate());
        }
        return predicate;
    }

    public QueryData query() {
        lexer.eatKeyword("select");
        List<String> fields = selectList();
        lexer.eatKeyword("from");
        Collection<String> tables = tableList();
        Predicate predicate = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            predicate = predicate();
        }
        return new QueryData(fields, tables, predicate);
    }


    public CreateViewData createView() {
        lexer.eatKeyword("view");
        String viewName = lexer.eatId();
        lexer.eatKeyword("as");
        QueryData queryData = query();
        return new CreateViewData(viewName, queryData);
    }

    public CreateIndexData createIndex() {
        lexer.eatKeyword("index");
        String idxName = lexer.eatId();
        lexer.eatKeyword("on");
        String tableName = lexer.eatId();
        lexer.eatDelimiter('(');
        String fieldName = field();
        lexer.eatDelimiter(')');
        return new CreateIndexData(idxName, tableName, fieldName);
    }

    public DeleteData delete() {
        lexer.eatKeyword("delete");
        lexer.eatKeyword("from");
        String tableName  = lexer.eatId();
        Predicate pred = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tableName, pred);
    }

    public InsertData insert() {
        lexer.eatKeyword("insert");
        lexer.eatKeyword("into");
        String tableName = lexer.eatId();
        lexer.eatDelimiter('(');
        List<String> fields = fieldList();
        lexer.eatDelimiter(')');
        lexer.eatKeyword("values");
        lexer.eatDelimiter('(');
        List<Constant> values = constantList();
        lexer.eatDelimiter(')');
        return new InsertData(tableName, fields, values);
    }

    public ModifyData modify() {
        lexer.eatKeyword("update");
        String tableName = lexer.eatId();
        lexer.eatKeyword("set");
        String fieldName = field();
        lexer.eatDelimiter('=');
        Expression newValue = expression();
        Predicate predicate = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            predicate = predicate();
        }
        return new ModifyData(tableName, fieldName, newValue, predicate);
    }

    public CreateTableData createTable() {
        lexer.eatKeyword("table");
        String tableName = lexer.eatId();
        lexer.eatDelimiter('(');
        Schema schema = fieldDefinitions();
        lexer.eatDelimiter(')');
        return new CreateTableData(tableName, schema);
    }

    private Schema fieldDefinitions() {
        Schema schema = fieldDefinition();
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            Schema secondSchema = fieldDefinitions();
            schema.addAll(secondSchema);
        }
        return schema;
    }

    private Schema fieldDefinition() {
        String fieldName = field();
        return fieldType(fieldName);
    }

    private Schema fieldType(String fieldName) {
        Schema schema = new Schema();
        if (lexer.matchKeyword("int")) {
            lexer.eatKeyword("int");
            schema.addIntField(fieldName);
        } else {
            lexer.eatKeyword("varchar");
            lexer.eatDelimiter('(');
            int strLength = lexer.eatIntConstant();
            lexer.eatDelimiter(')');
            schema.addStringField(fieldName, strLength);
        }
        return schema;
    }

    private List<String> fieldList() {
        List<String> fieldList = new ArrayList<>();
        fieldList.add(field());
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            fieldList.addAll(fieldList());
        }
        return fieldList;
    }

    private List<Constant> constantList() {
        List<Constant> constList = new ArrayList<>();
        constList.add(constant());
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            constList.addAll(constantList());
        }
        return constList;
    }

    private List<String> selectList() {
        List<String> fieldList = new ArrayList<>();
        fieldList.add(lexer.eatId());
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            fieldList.addAll(selectList());
        }
        return fieldList;
    }

    private Collection<String> tableList() {
        Collection<String> tables = new ArrayList<>();
        tables.add(lexer.eatId());
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',');
            tables.addAll(tableList());
        }
        return tables;
    }
}
