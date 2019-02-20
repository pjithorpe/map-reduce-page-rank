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
			Text mostRecent = null;
			long date = 0;
			long articleDate;
		    for (Text dAndLs : allDateAndLinks) {
		    	try {
					articleDate = utils.ISO8601.toTimeMS(dAndLs.toString().substring(0, 20)); //TODO: could move conversion to mapper?
				} catch (ParseException e) {
					e.printStackTrace();
					articleDate = 0;
				}
		    	
		    	if(articleDate > date) {
		    		date = articleDate;
		    		mostRecent = dAndLs; //get most recent article
		    	}
		    }
		    
		    rankAndLinks.set(mostRecent.toString().substring(20) + " 1");
		    context.write(key, rankAndLinks);
		}
	}