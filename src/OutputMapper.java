
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;

public class OutputMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
    	String[] val=value.toString().split("\t");
    	int n=Integer.parseInt(val[5]);
    	int m=Integer.parseInt(val[6]);
    	if(n<m)
    		{
    		output.write(new Text(val[5]+"\t"+val[6]+"\t"+val[2]),new Text());
    		}
    	else
    	{
    		output.write(new Text(val[6]+"\t"+val[5]+"\t"+val[2]),new Text());
    	}
         }
}

