
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.TaskID;

//here we are converting data into a uniform format of:
// u,v,wt,min,max,u,v
public class EdgeMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
	public static int v=EdgeReducer.vertices;
	public static int l=(int)Math.floor(v/2);
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
    	String[] val=value.toString().split("\t");
    	 	 	int u=Integer.parseInt(val[0]);
    	 	 	//int v=Integer.parseInt(val[1]);
    	 	 	if(u<=(int)Math.floor(v/2))
    	 	 	{
        		output.write(new IntWritable(u),new Text(val[1]+"\t"+val[2]+"\t"+
    	 	 	Edge1Reducer.ver[u-1][0]+"\t"+Edge1Reducer.ver[u-1][1]
    				+"\t"+val[0]+"\t"+val[1]));
    	 	 	}
    	 	 	else if(u>(int)Math.floor(v/2))
    	 	 	{
            		output.write(new IntWritable(u),new Text(val[1]+"\t"+val[2]+"\t"+
        	 	 	Edge2Reducer.ver[u-l-1][0]+"\t"+Edge2Reducer.ver[u-l-1][1]
        				+"\t"+val[0]+"\t"+val[1]));
        	 	 	}
    	 	 	
         }
}

