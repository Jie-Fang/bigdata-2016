import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by jie on 11/20/16.
 */
public class Count extends Configured implements Tool {
    static class CMapper extends Mapper<Text, Text, Text, Text> {
        static final Text exist = new Text("e");
        static final Text need = new Text("n");
        private String newKey;


        // Input is Sequence Text
        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String keyStr = key.toString();
            String[] values = value.toString().split(" ");
            if (values.length != 0) {
                for (int i = 0; i < values.length; ++i) {
                    if (values[i].compareTo(keyStr) > 0)
                        newKey = keyStr + values[i];
                    else
                        newKey = values[i] + keyStr;
                    context.write(new Text(newKey), exist);
                    for (int j = i + 1; j < values.length; ++j) {
                        if (values[i].compareTo(values[j]) > 0)
                            newKey = values[j] + values[i];
                        else
                            newKey = values[i] + values[j];
                        context.write(new Text(newKey), need);
                    }
                }
            }
        }
    }

    // Reduce number must be 1
    static class CReducer extends Reducer<Text, Text, Text, Text> {
        private static long totalCount = 0;
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean existState = false;
            long needCount = 0;
            for (Text value : values) {
                if (!existState && value.toString().equals("e"))
                    existState = true;
                if (value.toString().equals("n"))
                    ++needCount;
            }
            totalCount += needCount;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("The total number of triangle:"), new Text(Long.toString(totalCount)));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set(MRJobConfig.NUM_MAPS, "10");
        Job job1 = Job.getInstance(conf);
        Job job2 = Job.getInstance(conf);
        job1.setJarByClass(GraphBuilder.class);
        job1.setMapperClass(GraphBuilder.GBMapper.class);
        job1.setReducerClass(GraphBuilder.GBReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        job2.setJarByClass(Count.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(CMapper.class);
        job2.setReducerClass(CReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        return (job2.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new Count(), args);
    }
}
