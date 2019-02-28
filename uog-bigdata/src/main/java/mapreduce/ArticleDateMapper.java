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
	boolean skipRecord = false;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		long maxDate = Long.parseLong(context.getConfiguration().get("dateLimit", "0"));
		String[] splitString = value.toString().split("\\s");
		String word = null;
		
		if(splitString[0].equals("REVISION")) {
			skipRecord = false;
			valueSB = new StringBuilder();
			long date = 0;
			try {
				date = utils.ISO8601.toTimeMS(splitString[4]);
			} catch (ParseException e) {
				e.printStackTrace();
				date = -1;
			}
			
			if(date < maxDate && date > 0) {
				title.set(splitString[3]); // set key
				valueSB.append(Long.toString(date)); // date at start
				valueSB.append(" ");
			}
			else {
				skipRecord = true;
			}
			
		}
		else if (splitString[0].equals("MAIN") && !skipRecord) {
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