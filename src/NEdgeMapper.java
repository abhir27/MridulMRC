
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;

public class NEdgeMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
    	String[] val=value.toString().split("\t");
    	 	 	int n=Integer.parseInt(val[0]);
        		output.write(new IntWritable(n),new Text(val[1]+"\t"+val[2]+"\t"+OneReducer.ver[n][0]+"\t"+OneReducer.ver[n][1]
    				+"\t"+val[5]+"\t"+val[6]));
         }
}

