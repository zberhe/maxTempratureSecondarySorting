import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MaxPerDayReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException,InterruptedException{
        int maxValue = Integer.MIN_VALUE;
       for(IntWritable val : values){
           maxValue = Math.max(val.get(),maxValue);
       }
       context.write(key,new IntWritable(maxValue));

    }
}
