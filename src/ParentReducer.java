//package VoteCountApplication;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParentReducer extends Reducer< IntWritable, Text, IntWritable, Text> {
    //here we are doing tree jumping
	//P (u) := min{P'(u),min{P'(v)|P (v) = u}}
	public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    int vmin=MinReducer.Pdash[key.get()-1];	
    int l=(int)Math.floor(MridulMRC.v/2);
   for(int i=0;i<l;i++)
 {
	   if(Comp1Reducer.P[i]==key.get())
	   {
		  if(vmin>MinReducer.Pdash[i])
		  {
			  vmin=MinReducer.Pdash[i];
		  }
	   }
 }
     
   Comp1Reducer.P[key.get()-1]=vmin;
   for(Text val:values)
   {
	   output.write(key, val);
   }
   }
	
}