import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MeanMaxDriver extends Configured implements Tool {
    private static final Path OUTPUT_PATH = new Path("temp");
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job1 = new Job(getConf(), "Max temperature");
        FileSystem hdfs = FileSystem.get(job1.getConfiguration());

        // delete existing directory
        if (hdfs.exists(OUTPUT_PATH)) {
            hdfs.delete(OUTPUT_PATH, true);
        }

        job1.setJarByClass(getClass());
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, OUTPUT_PATH);
        job1.setMapperClass(MaxPerDayMapper.class);
        job1.setReducerClass(MaxPerDayReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
         job1.waitForCompletion(true);
        System.out.println("========= Job2 ==============");
        Job job2 = new Job(getConf(), "Mean Max temperature");
        job2.setJarByClass(getClass());
        FileInputFormat.addInputPath(job2, OUTPUT_PATH);//previous output path
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.setMapperClass(MeanMaxMapper.class);
        job2.setReducerClass(MeanMaxReducer.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        boolean exitCode = job2.waitForCompletion(true) ;
            //delete temp directory - revise(not the best way)

         hdfs = FileSystem.get(job2.getConfiguration());

        // delete existing directory
        if (hdfs.exists(OUTPUT_PATH)) {
            hdfs.delete(OUTPUT_PATH, true);
        }
                return exitCode ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MeanMaxDriver(), args);
        System.exit(exitCode);
    }
}
