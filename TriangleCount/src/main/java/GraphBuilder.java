import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by jie on 11/20/16.
 */
public class GraphBuilder {
    static class GBMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] strs = value.toString().split(" ");
            if (!strs[0].equals(strs[1]))
                context.write(new Text(strs[0]), new Text(strs[1]));
        }
    }

    static class GBReducer extends Reducer<Text, Text, Text, Text> {
        private StringBuilder newValue = new StringBuilder();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values)
                newValue.append(value.toString() + " ");
            context.write(key, new Text(newValue.toString()));
            newValue = new StringBuilder();
        }
    }

}
