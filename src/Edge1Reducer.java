//package VoteCountApplication;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Here we are calculating min and max value for each vertex and keeping count of number of vertices in variable vertices.
public class Edge1Reducer extends Reducer< IntWritable, Text, IntWritable, Text> {
	public static int v=EdgeReducer.vertices;
	public static int[][] ver=new int[(int)Math.floor(v/2)][2];
	public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    	int min=999;int max=0;
   for(Text value:values)
 {
	String s[]=value.toString().split("\t");
	int n=Integer.parseInt(s[1]);
	if(min>n)min=n;
	if(max<n)max=n;
	ver[key.get()-1][0]=min;
	ver[key.get()-1][1]=max;
	output.write(key, value);
 }
  
    }
}