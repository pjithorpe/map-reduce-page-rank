package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PageRankCalculationMapper extends Mapper<Text, Text, Text, Text> {
	Text link = new Text();
	Text sourceAndPR = new Text();
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\\s");
		int noOfLinks = line.length - 1;
		float pr = Float.parseFloat(line[noOfLinks]); //score is at the end of the string
		
		StringBuilder links = new StringBuilder("");
		if(noOfLinks > 0) {
			float prFraction = pr / noOfLinks;
			for(int i=0; i<noOfLinks; i++) { //(ignore last entry as this is the score)
				links.append(line[i]);
				links.append(" ");
				link.set(line[i].toString());
				sourceAndPR.set(Float.toString(prFraction));
				context.write(link, sourceAndPR); //key: title of link, value: source title, value to contribute to link's pr
			}
		}
		context.write(key, new Text(key.toString() + " " + links.toString()));
	}
}