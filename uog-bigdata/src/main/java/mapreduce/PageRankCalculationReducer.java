package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PageRankCalculationReducer<Key> extends Reducer<Key, Text, Key, Text> {

	Text title = new Text();
	Text linksAndPR = new Text();

	protected void reduce(Key key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		float pr = 0.0f;
		StringBuilder sb = new StringBuilder("");
		for (Text val : values) {
			String[] transfer = val.toString().split("\\s");
			try {
				if(transfer[0].equals(key.toString())) { //signifies original key and links, remember to write back to file
					if(context.getConfiguration().get("lastIter", "0") != "1") { //if this is the last iteration, just write the page ranks
						title.set(transfer[0]);
						for(int i=1; i<transfer.length; i++) {
							sb.append(transfer[i]);
							sb.append(" ");
						}
					}
				}
				else {
					pr += Float.parseFloat(transfer[0]);
				}
			}
			catch(Exception e) {
				System.out.println("A value for the reducer is badly formatted.");
			}
		}
		
		pr = (pr * 0.85f) + 0.15f; //dampening factor
		linksAndPR.set(sb.toString() + " " + Float.toString(pr));
		context.write(key, linksAndPR);
	}
}