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
}
