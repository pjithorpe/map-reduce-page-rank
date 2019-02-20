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

public class PageRankCalculationMapper extends Mapper<Text, Text, Text, Text> { // The main map() function; the input
																				// key/value classes must match the
																				// first two above, and the key/value
																				// classes in your emit() statement must
																				// match the latter two above.
	Text link = new Text();
	Text sourceAndPRAndLinkCount = new Text();
	/**
	 * extended from example at
	 * "https://coe4bd.github.io/HadoopHowTo/stringMultipleValues/stringMultipleValues.html"
	 * * An example of how to pass multiple values from a mapper to a reducer in a *
	 * single string value via string concatenation.
	 */
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
				sourceAndPRAndLinkCount.set(Float.toString(prFraction));
				context.write(link, sourceAndPRAndLinkCount); //key: title of link, value: source title, source page-rank, noOfLinks from source
			}
		}
		context.write(key, new Text(key.toString() + " " + links.toString()));
		//context.write(key, new Text(links.toString())); //also send copy of original input so that reducer sets up the same articles and links again
		
		/*String line = value.toString();
		String[] field = line.split(" ");
		float pr = Float.parseFloat(field[field.length-1]);
		field = Arrays.copyOf(field, field.length - 1);
		float prFraction = pr / field.length;
		StringBuilder original = new StringBuilder("<");
		for(String link : field) {
			original.append(link.toString());
			original.append(" ");
		}
		original.append(">");
		String originalFinal = original.toString();
		textKey.set(key);
		textValue.set(originalFinal);
		context.write(textKey, textValue);
		//System.out.println("key: " + key + " value: " + originalFinal);
		textValue.set(String.valueOf(prFraction));
		for (String outlink : field) {
			textKey.set(outlink);
			context.write(textKey, textValue);
			//System.out.println("key: " + outlink + " value: " + textValue.toString());
		}*/
	}
}