package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArticleDateMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text title = new Text();
	private Text dateAndLinks = new Text();
	private StringBuilder valueSB = new StringBuilder();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splitString = value.toString().split("\\s");
		String word = null;
		
		if(splitString[0].equals("REVISION")) {
			valueSB = new StringBuilder();
			title.set(splitString[3]); // set key
			valueSB.append(splitString[4]); // date at start
			valueSB.append(" ");
		}
		else if (splitString[0].equals("MAIN")) {
			int ssPos = 1;
			while (ssPos < splitString.length) {
				word = splitString[ssPos];
				if (!word.equals(title.toString()) && !valueSB.toString().contains(word)) { // no self-links TODO: rethink second part, as it's pretty inefficient (might be achievable with an additional mapreduce?)
					valueSB.append(word); // add link
					valueSB.append(" ");
				}
				ssPos++;
			}
			
			dateAndLinks.set(valueSB.toString()); // set value
			context.write(title, dateAndLinks);
		}

		// System.out.println("LOOK HERE!!!!!! " + "title: " + title.toString() + "
		// date: " + debugDate + " links: " + Integer.toString(debugCount));
	}
}