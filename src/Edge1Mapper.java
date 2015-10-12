
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;
public class Edge1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    public static int l=EdgeReducer.vertices;
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
    	String s[]=value.toString().split("\t");
    	if(Integer.parseInt(s[0])<=(int)Math.floor(l/2))
    	{
    		output.write(new IntWritable(Integer.parseInt(s[0])),new Text(s[1]+"\t"+s[2]));
    	}
          }
}

