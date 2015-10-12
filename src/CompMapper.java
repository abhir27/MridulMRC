
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CompMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	//Here we are separating matched edges to do further computation 
			@Override
       public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
		String[] s=value.toString().split("\t");
		//s=0,u,v,wt,min,max,u,v
		if(Integer.parseInt(s[0])==0)
		{
			output.write(new IntWritable(Integer.parseInt(s[1])), 
					new Text(s[2]+"\t"+s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[6]+"\t"+s[7]));
		}
		
    }
}

