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

		StringBuilder full = new StringBuilder();
		for (Text val : values) {
			full.append(val);
			full.append(" ");
		}
		String full2 = full.toString();
		String original = StringUtils.substringBetween(full2, "<", ">");
		full2.replaceAll("#\\<.*?>", "");

		String[] out = original.split(" ", 2);
		String[] prs = full2.split(" ");
		float pr = 0.0f;
		for (String val : prs) {
			pr = pr + Float.parseFloat(val);
		}
		pr = (pr * 0.85f) + 0.15f;
		textValue.set(out[1] + " " + pr);
		context.write(key, textValue);
	}
}