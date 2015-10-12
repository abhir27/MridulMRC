//package VoteCountApplication;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Here we are calculating min and max value for each vertex and keeping count of number of vertices in variable vertices.
public class EdgeReducer extends Reducer< IntWritable, Text, IntWritable, Text> {
public static int vertices=0;
    public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    for(Text value:values)
 {

	output.write(key, value);
 }
     vertices++;   
    }
}