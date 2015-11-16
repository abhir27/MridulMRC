
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;


//In this job we are are duplicating edges
//If we given with edge u,v,w as start of edge,end of edge,weight we add a duplicate edge v,u,w
//Also we are keeping count of edges in variable edges.
public class EdgeMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	public static int edges=1;
	//public static final float epsilon=0.25f;
    @Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
    	String[] val=value.toString().split(",");
   double prob= Math.round(Math.random());
   double wt=Integer.parseInt(val[2])+prob;
    	 	output.write(new IntWritable(Integer.parseInt(val[1])),new Text(val[0]+"\t"+wt));
    		output.write(new IntWritable(Integer.parseInt(val[0])),new Text(val[1]+"\t"+wt));
    	   	edges++;
          }
}

