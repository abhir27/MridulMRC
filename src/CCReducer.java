//package VoteCountApplication;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CCReducer extends Reducer< IntWritable, Text, IntWritable, Text> {
	public static int components=0;
	//counting no. 0f components
	//incrementing counter for each distinct vertex
    public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    	components++;
    }
}