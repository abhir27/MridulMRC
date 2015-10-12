

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class GraphReducer extends Reducer<IntWritable, Text,IntWritable, Text > {
	public static int[] cc=new int[EdgeReducer.vertices];
	public static int x=MridulMRC.loopid;
	public static int cid=1; 
	public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
		
			for(Text val:values)
			{
				String[] s=val.toString().split("\t");
				if(Integer.parseInt(s[0])==0)
				{
					if(cc[Integer.parseInt(s[1])-1]==0)
					{
					cc[Integer.parseInt(s[1])-1]=cid;
					cid++;
					}
					cc[Integer.parseInt(s[2])-1]=cc[Integer.parseInt(s[1])-1];
					output.write(new IntWritable(Integer.parseInt(s[0])), 
							new Text(cc[Integer.parseInt(s[1])]+"\t"+cc[Integer.parseInt(s[2])-1]+"\t"+
							s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[1]+"\t"+s[2]));
					}
				else
				{
					output.write(new IntWritable(Integer.parseInt(s[0])), 
							new Text(cc[Integer.parseInt(s[1])]+"\t"+cc[Integer.parseInt(s[2])-1]+"\t"+
							s[3]+"\t"+s[4]+"\t"+s[5]+"\t"+s[1]+"\t"+s[2]));
						
				}
			}
	}
}