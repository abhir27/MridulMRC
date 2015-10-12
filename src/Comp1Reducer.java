//package VoteCountApplication;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Comp1Reducer extends Reducer< IntWritable, Text, IntWritable, Text> {
public static int l=(int)Math.floor(MridulMRC.v/2);
public static int[] P=new int[l];
//Here we are calculating minimum vertex connected with a given vertex
//P(u)=min(u,min(v|v is neighbor of u))
    public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    	//System.out.println("l in comp1"+l);
    	int vmin=key.get();
    	String s[];
    for(Text value:values)
 {
	s=value.toString().split("\t");
	if(vmin>Integer.parseInt(s[0]))vmin=Integer.parseInt(s[0]);
	//u-key,value-v,wt,min,max,u,v
	output.write(key, new Text(s[0]+"\t"+s[1]+"\t"+s[2]+"\t"+s[3]+"\t"+s[4]+"\t"+s[5]));
 }
   P[key.get()-1]=vmin; 
   
    }
}