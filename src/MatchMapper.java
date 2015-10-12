
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MatchMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	//From previous input we are taking data and check for each edge if its weight==min if yes add its status=0
	//else add 1 as status
	//If status==0 it will be stored as intermediate input and remaining data marked with 1 is sent to next pass.
		@Override
       public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
		String[] s=value.toString().split("\t");
		if(Integer.parseInt(s[2])==Integer.parseInt(s[3]))
		{
			output.write(new IntWritable(0), new Text(value.toString()));
		}
		else
		{
			output.write(new IntWritable(1),new Text(value.toString()));
		}
    }
}

