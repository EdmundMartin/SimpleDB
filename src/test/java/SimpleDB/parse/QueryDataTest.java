package SimpleDB.parse;

import SimpleDB.query.Predicate;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

public class QueryDataTest {

    @Test
    public void test_ToString() {
        QueryData query = new QueryData(List.of("name", "surname"), List.of("students"), new Predicate());
        String result = query.toString();
        System.out.println(result);
        assertEquals("select name, surname from students", result);
    }
}
