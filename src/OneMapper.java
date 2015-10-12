
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class OneMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	//public static int newedges=0;
		@Override
       public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
		String[] s=value.toString().split("\t");
		if(Integer.parseInt(s[0])==1)
		{
			output.write(new IntWritable(Integer.parseInt(s[0])), new Text(s[1]+"\t"+s[2]+"\t"+s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[6]+"\t"+s[7]));
		}
		//newedges++;
    }
}

