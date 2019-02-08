package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyFirstMapReduceJob extends Configured implements Tool {

	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
	@InterfaceAudience.Public
	@InterfaceStability.Stable
	static class MyFirstMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
		    while (itr.hasMoreTokens()) {
		      word.set(itr.nextToken());
		      context.write(word, one);
		    }
		}
	}

	// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
	@InterfaceAudience.Public
	@InterfaceStability.Stable
	static class MyFirstReducer<Key> extends Reducer<Key, IntWritable, Key, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		// The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		// Make sure that the output key/value classes also match those set in your job's configuration (see below).
		@Override
		protected void reduce(Key key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
		    for (IntWritable val : values) {
		      sum += val.get();
		    }
		    result.set(sum);
		    context.write(key, result);
		}
	}

	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf(), "MyFirstMapReduceJob");

		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(MyFirstMapReduceJob.class);

		// 2. Set mapper and reducer classes
		job.setMapperClass(MyFirstMapper.class);

		// 3. Set input and output format, mapper output key and value classes, and final output key and value classes
		job.setReducerClass(MyFirstReducer.class);

		// 4. Set input and output paths; remember, these will be HDFS paths or URLs
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 5. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
		job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 14);

		// 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		boolean succeeded = job.waitForCompletion(true);
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyFirstMapReduceJob(), args));
	}
}
