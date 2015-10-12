//package VoteCountApplication;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OutputReducer extends Reducer< Text, Text, Text, Text> {
	
    public void reduce(Text key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    	output.write(key, new Text());
    	
    }
}