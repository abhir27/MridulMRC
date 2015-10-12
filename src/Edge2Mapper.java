
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;


//In this job we are are duplicating edges
//If we given with edge u,v,w as start of edge,end of edge,weight we add a duplicate edge v,u,w
//Also we are keeping count of edges in variable edges.
public class Edge2Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	 public static int l=EdgeReducer.vertices;
    @Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
    	String s[]=value.toString().split("\t");
    	if(Integer.parseInt(s[0])>(int)Math.floor(l/2))
    	{
    		output.write(new IntWritable(Integer.parseInt(s[0])),new Text(s[1]+"\t"+s[2]));
    	}
}
}
