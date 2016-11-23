import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.After;
import org.junit.Before;

/**
 * Created by jie on 11/21/16.
 */
public class GraphBuilderTest {
    private Mapper mapper;
    private Reducer reducer;
    private MapReduceDriver driver;

    @Before
    public void init() {
        mapper = new GraphBuilder.GBMapper();
        reducer = new Reducer();
        driver = new MapReduceDriver(mapper, reducer);
    }

    @After
    public void test() {
        driver.withInput(null, new Text("A B"));
        driver.withInput(null, new Text("A E"));
        driver.withInput(null, new Text("B A"));
        driver.withInput(null, new Text("B C"));
        driver.withInput(null, new Text("B E"));
        driver.withInput(null, new Text("C B"));
        driver.withInput(null, new Text("C E"));
        driver.withInput(null, new Text("C D"));
        driver.withInput(null, new Text("D C"));
        driver.withInput(null, new Text("D E"));
        driver.withInput(null, new Text("E A"));
        driver.withInput(null, new Text("E B"));
        driver.withInput(null, new Text("E C"));
        driver.withInput(null, new Text("E D"));
        driver.withOutput(new Text("A"), new Text("B E"));
        driver.withOutput(new Text("B"), new Text("A C E"));
        driver.withOutput(new Text("C"), new Text("B D E"));
        driver.withOutput(new Text("D"), new Text("C E"));
        driver.withOutput(new Text("E"), new Text("A B C D"));
        driver.runTest();
    }
}
