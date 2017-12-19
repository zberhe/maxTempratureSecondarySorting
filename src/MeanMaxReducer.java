import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MeanMaxReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException,InterruptedException{
       int sum=0;
       int len=0;
       for(IntWritable val : values){
           len++;
           sum +=val.get();
       }
       int mean=sum/len;
       context.write(key,new IntWritable(mean));

    }
}
