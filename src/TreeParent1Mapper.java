
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;

public class TreeParent1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	 @Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
    	String[] s=value.toString().split("\t");
    	//s=u,v,wt,min,max,u,v
    	 	output.write(new IntWritable(Integer.parseInt(s[0])), 
    	 			new Text(s[1]+"\t"+s[2]+"\t"+s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[6]));
          }
}

