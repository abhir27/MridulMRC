
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class ChangeToCIdMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
public static int l=(int)Math.floor(MridulMRC.v/2);
//In this job we are changing u and v of edge with its connected component id 
//Also we drop cases when both u and v are in same component.
		@Override
       public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
		String[] s=value.toString().split("\t");
		if(Integer.parseInt(s[0])==1)
		{
			if(Integer.parseInt(s[1])<=l && Integer.parseInt(s[2])<=l){
			output.write(new IntWritable(Integer.parseInt(s[0])), new Text(Comp1Reducer.P[Integer.parseInt(s[1])-1]
					+"\t"+Comp1Reducer.P[Integer.parseInt(s[2])-1]+"\t"+s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[6]+"\t"+s[7]));
		}
			else if(Integer.parseInt(s[1])>l && Integer.parseInt(s[2])>l)
			{
				output.write(new IntWritable(Integer.parseInt(s[0])), new Text(Comp2Reducer.P[Integer.parseInt(s[1])-l-1]
						+"\t"+Comp2Reducer.P[Integer.parseInt(s[2])-l-1]+"\t"+s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[6]+"\t"+s[7]));
			}
			else if(Integer.parseInt(s[1])<=l && Integer.parseInt(s[2])>l){
				output.write(new IntWritable(Integer.parseInt(s[0])), new Text(Comp1Reducer.P[Integer.parseInt(s[1])-1]
						+"\t"+Comp2Reducer.P[Integer.parseInt(s[2])-l-1]+"\t"+s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[6]+"\t"+s[7]));
			}
			else
			{
				output.write(new IntWritable(Integer.parseInt(s[0])), new Text(Comp2Reducer.P[Integer.parseInt(s[1])-l-1]
						+"\t"+Comp1Reducer.P[Integer.parseInt(s[2])-1]+"\t"+s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[6]+"\t"+s[7]));
			}
		}
		
    }
}

