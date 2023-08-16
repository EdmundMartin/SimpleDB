package SimpleDB.parse;

import java.util.List;

public class QueryDataTest {

    public static void main(String[] args) {
        QueryData q = new QueryData(List.of("Something", "Edmund"), null, null);

        System.out.println(q.toString());
    }
}
