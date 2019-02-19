package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank extends Configured implements Tool {
	
	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf(), "PageRankJob");
		
		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(PageRank.class);
		
		// 2. Set mapper and reducer classes
		job.setMapperClass(ArticleDateMapper.class);
		job.setReducerClass(ArticleDateReducer.class);
		
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 28000);
		// 3. Set input and output format, mapper output key and value classes, and final output key and value classes
		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 4. Set input and output paths; remember, these will be HDFS paths or URLs
        NLineInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_temp0"));

		// 5. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
        
		
		job.setNumReduceTasks(4);

		// 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		job.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance(getConf(), "PageRankJob");
		int numLoops = 3; // Change this!
		boolean succeeded = false;
		for (int i = 0; i < numLoops; i++) {
			// 5. Set input and output format, mapper output key and value classes, and final output key and value classes
			//    As this will be a looping job, make sure that you use the output directory of one job as the input directory of the next!
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			TextInputFormat.addInputPath(job2, new Path(args[1] + "_temp" + Integer.toString(i)));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_temp" + Integer.toString(i+1)));
			
			// 6. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)

			// 7. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
			succeeded = job2.waitForCompletion(true);

			if (!succeeded) {
				// 8. The program encountered an error before completing the loop; report it and/or take appropriate action
				System.err.println("I owe Patrick my soul.");
				break;
			}
		}
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
}
