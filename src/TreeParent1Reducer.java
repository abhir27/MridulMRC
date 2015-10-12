//package VoteCountApplication;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TreeParent1Reducer extends Reducer< IntWritable, Text, IntWritable, Text> {
    public static boolean PequalOldP=true;
    //here we are doing normal tree jumping
    //we are finding parent using
    //P(u)=P(P(u))
    //Also here we are checking if all elements are equal in both arrays P and OldP using a boolean variable PequalOldP
    //If both values are same for an element then AND "true" with PequalOldP
    //else AND "PequalOldP" with PequalOldP
    //this way even if a single element is different output will be false.
    //This variable helps us in determining whether we need to continue in loop or break out of it.
	public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    int l=(int)Math.floor(MridulMRC.v/2);
    int temp;
    if(key.get()<=l)
    {
    	
    	int v=Comp1Reducer.P[key.get()-1];
    	Comp1Reducer.P[key.get()-1]=Comp1Reducer.P[v-1];
    	temp=Comp1Reducer.P[key.get()-1];
    	PequalOldP=PequalOldP &&(Comp1Reducer.P[key.get()-1]==MinReducer.oldP[key.get()-1]);
    }
    else
    {
    	int v=Comp2Reducer.P[key.get()-l-1];
    	if(v<=l){
    	Comp2Reducer.P[key.get()-l-1]=Comp1Reducer.P[v-1];
    	temp=Comp2Reducer.P[key.get()-l-1];
    	}
    	else
    	{
    		Comp2Reducer.P[key.get()-l-1]=Comp2Reducer.P[v-l-1];
    		temp=Comp2Reducer.P[key.get()-l-1];
    	}
    	
    	PequalOldP=PequalOldP &&(Comp2Reducer.P[key.get()-l-1]==Min2Reducer.oldP[key.get()-l-1]);
    }   
   // System.out.println("p==oldP"+PequalOldP);
    	output.write(key,new Text(Integer.toString(temp)));
   
	}
}