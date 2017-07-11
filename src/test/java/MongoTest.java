import com.semsoft.Mongo;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by breynard on 28/04/17.
 */
public class MongoTest {
    @Ignore
    @Test
    public void test() throws IOException, URISyntaxException {
        Mongo.main(new String[]{"mongodb://localhost:27017/", getClass().getResource("test.csv").toURI().getPath(), "many"});
    }


    @Test
    public void testAstria_Many() throws IOException, URISyntaxException {
        Mongo.main(new String[]{"mongodb://localhost:27017/", getClass().getResource("astria.csv").toURI().getPath(), "many", "astria"});
    }

    @Test
    public void testAstria_Bulk() throws IOException, URISyntaxException {
        Mongo.main(new String[]{"mongodb://localhost:27017/", getClass().getResource("astria.csv").toURI().getPath(), "bulk", "astria"});
    }

    @Test
    public void testAstria_Async() throws IOException, URISyntaxException {
        Mongo.main(new String[]{"mongodb://localhost:27017/", getClass().getResource("astria.csv").toURI().getPath(), "async", "astria"});
    }
}
