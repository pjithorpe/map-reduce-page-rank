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

	Text textValue = new Text();

	protected void reduce(Key key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuilder pageRanks = new StringBuilder();
		String original = null;
		for (Text val : values) {
			String valStr = val.toString();
			if(valStr.startsWith("<")) {
				original = valStr.substring(1, valStr.length() - 1);
			}
			else {
				pageRanks.append(valStr);
				pageRanks.append(" ");
			}
		}
		
		String[] prs = pageRanks.toString().split(" ");
		float pr = 0.0f;
		for (String val : prs) {
			if(val.length() != 0) {
				pr = pr + Float.parseFloat(val);
			}
		}
		pr = (pr * 0.85f) + 0.15f;
		textValue.set(original + " " + pr);
		context.write(key, textValue);
	}
}