import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MeanMaxMapper extends Mapper <Text, Text, Text, IntWritable>{
    //0290709999919010101 - key format station_id+year+day
    @Override
    public void map(Text key, Text value,Context context) throws IOException, InterruptedException {
    String stationIDdate = key.toString();
    String stationId = stationIDdate.substring(0,11);
    String day = stationIDdate.substring(15);
    context.write(new Text(stationId+day), new IntWritable(Integer.parseInt(value.toString())));

    }
}
