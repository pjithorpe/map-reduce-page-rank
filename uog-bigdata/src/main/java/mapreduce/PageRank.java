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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank extends Configured implements Tool {
	
	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf(), "GetLinkedPagesJob");
		
		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(PageRank.class);
		
		// 2. Set mapper and reducer classes
		job.setMapperClass(ArticleDateMapper.class);
		job.setReducerClass(ArticleDateReducer.class);
		
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 14000);
		// 3. Set input and output format, mapper output key and value classes, and final output key and value classes
		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 4. Set input and output paths; remember, these will be HDFS paths or URLs
        NLineInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 5. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
        
		
		job.setNumReduceTasks(4);

		// 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		boolean succeeded = job.waitForCompletion(true);
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
}
