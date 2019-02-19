package mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyLoopingMapReduceJob extends Configured implements Tool {

	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}

		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		Text textKey = new Text();
		Text textValue = new Text();
		FloatWritable floatWritable = new FloatWritable();
		
		/** extended from example at "https://coe4bd.github.io/HadoopHowTo/stringMultipleValues/stringMultipleValues.html"
		* An example of how to pass multiple values from a mapper to a reducer in a
		* single string value via string concatenation.
		*/
		
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		    String line = value.toString();
		    String[] field = line.split(" ");
		    float pr = Float.parseFloat(field[-1]);
		    float prFraction = pr/field.length;
		    field = Arrays.copyOf(field, field.length-1);
		    StringBuilder original = new StringBuilder("<");
		    original.append(key.toString());
			original.append(" ");
		    original.append(Arrays.toString(field));
		    original.append(">");
			String originalFinal = original.toString();
			textKey.set(key);
		    textValue.set(originalFinal);
		    context.write(textKey, textValue);
		    textValue.set(String.valueOf(prFraction));
		    
		    for (String outlink : field) {
		    	textKey.set(outlink);
		    	context.write(textKey, textValue);
			  }
		    }
		}
	

	// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyReducer<Key> extends Reducer<Key, Text , Key, Text > {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}
		

		Text textValue = new Text();
		
		// The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		// Make sure that the output key/value classes also match those set in your job's configuration (see below).
		@Override
		protected void reduce(Key key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  //String keyStr = key.toString();			  
			  StringBuilder full = new StringBuilder();
			  for (Text val : values) {
			    full.append(val);
			  }
			  String full2 = full.toString();
			  String original = StringUtils.substringBetween(full2, "<", ">");
			  full2.replaceAll("#\\<.*?>", "");		
			  //Text textKey = new Text();
			  String[] out = original.split(" ", 2);
			  String[] prs = full2.split(" ");
			  float pr = 0.0f;
			  for (String val : prs) {
				  pr = pr + Integer.parseInt(val);
			  }
			  pr = (pr * 0.85f) + 0.15f;
			  textValue.set(out[1] + " " + pr);
			  context.write(key, textValue);
			}
			 
		
		
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf());

		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(MyLoopingMapReduceJob.class);

		// 2. Set mapper and reducer classes
		// ...

		// 3. Set final output key and value classes
		// ...

		// 4. Get #loops from input (args[])
		int numLoops = 0; // Change this!

		boolean succeeded = false;
		for (int i = 0; i < numLoops; i++) {
			// 5. Set input and output format, mapper output key and value classes, and final output key and value classes
			//    As this will be a looping job, make sure that you use the output directory of one job as the input directory of the next!
			// ...
			
			// 6. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
			// ...

			// 7. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
			succeeded = job.waitForCompletion(true);

			if (!succeeded) {
				// 8. The program encountered an error before completing the loop; report it and/or take appropriate action
				// ...
				break;
			}
		}
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyLoopingMapReduceJob(), args));
	}
}
