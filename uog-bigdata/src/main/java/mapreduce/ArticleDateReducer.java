package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ArticleDateReducer<Key> extends Reducer<Key, Text, Key, Text> {
		
		private Text rankAndLinks = new Text();
		
		@Override
		protected void reduce(Key key, Iterable<Text> allDateAndLinks, Context context) throws IOException, InterruptedException {
			String mostRecent = null;
			long date = 0;
			long articleDate;
		    for (Text dAndLs : allDateAndLinks) {
		    	String dateStr = dAndLs.toString().split("\\s")[0];
		    	articleDate = Long.parseLong(dateStr);
		    	
		    	if(articleDate > date) {
		    		date = articleDate;
		    		mostRecent = dAndLs.toString().substring(dateStr.length() + 1); //get most recent article
		    	}
		    }
		    
		    rankAndLinks.set(mostRecent + " 1");
		    context.write(key, rankAndLinks);
		}
	}