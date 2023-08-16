package SimpleDB.parse;

public class CreateViewData {
    private final String viewName;
    private final QueryData queryData;

    public CreateViewData(String viewName, QueryData queryData) {
        this.viewName = viewName;
        this.queryData = queryData;
    }

    public String viewName() {
        return viewName;
    }

    public String viewDefinition() {
        return queryData.toString();
    }
}
