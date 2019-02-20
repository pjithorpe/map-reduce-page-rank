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

public class PageRankCalculationMapper extends Mapper<LongWritable, Text, Text, Text> {

	// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
	Text textKey = new Text();
	Text textValue = new Text();
	FloatWritable floatWritable = new FloatWritable();
	
	/** extended from example at "https://coe4bd.github.io/HadoopHowTo/stringMultipleValues/stringMultipleValues.html"
	* An example of how to pass multiple values from a mapper to a reducer in a
	* single string value via string concatenation.
	*/
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] t = line.split(" ",2);
		String realKey = t[0];
		String[] field = t[1].split(" ");
		float pr = Float.parseFloat(field[-1]);
		field = Arrays.copyOf(field, field.length - 1);
		float prFraction = pr / field.length;
		StringBuilder original = new StringBuilder("<");
		original.append(realKey);
		original.append(" ");
		original.append(Arrays.toString(field));
		original.append(">");
		String originalFinal = original.toString();
		textKey.set(realKey);
		textValue.set(originalFinal);
		context.write(textKey, textValue);
		textValue.set(String.valueOf(prFraction));

		for (String outlink : field) {
			textKey.set(outlink);
			context.write(textKey, textValue);
		}
	}
}