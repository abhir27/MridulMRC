//package VoteCountApplication;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinReducer extends Reducer< IntWritable, Text, IntWritable, Text> {
public static int[] Pdash=new int[(int)Math.floor(MridulMRC.v/2)];
public static int[] oldP=new int[(int)Math.floor(MridulMRC.v/2)];
//here we are copying data from P to another array OldP
//Also we are finding P' where
//P'(u)= P (min{P(u),min{P(v)|vertex v is adjacent to vertex u in G }}
    public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    	int l=(int)Math.floor(MridulMRC.v/2);
    	int vmin=Comp1Reducer.P[key.get()-1];//u
   for(Text value:values)
 {
	    String[] s=value.toString().split("\t");
	    int v=Integer.parseInt(s[0]);
	int n;
	if(v<=l){
	 n=Comp1Reducer.P[v-1];//v
	if(n<vmin)vmin=n;
	}

	output.write(key,value);
 }
   Pdash[key.get()-1]=vmin;
  oldP[key.get()-1]=Comp1Reducer.P[key.get()-1];
    }
}