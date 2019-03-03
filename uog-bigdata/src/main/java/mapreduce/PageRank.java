package mapreduce;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.File;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank extends Configured implements Tool {
	
	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
        
		//read input args
		String inputPath = null;
        String outputPath = null;
        String itersStr = null;
        String dateStr = null;
        Configuration conf = new Configuration();
		try {
			inputPath = args[0];
	        outputPath = args[1];
	        itersStr = args[2];
	        dateStr = args[3];
		}
		catch(Exception e) {
			System.out.println("Failed to read arguments. " + e.getMessage());
			System.exit(0);
		}
        
		long lineCount = 0;
		int mapperCount = 20;
		int nLineCount = 0;
		if(inputPath != null) {
			try {
				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
				//conf.set("fs.defaultFS","file:///"); <-- to run locally
				FileSystem fs = FileSystem.get(conf);
				Path path = new Path(inputPath);
				FSDataInputStream inputStream = fs.open(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                String line = reader.readLine();

                while(line!=null){
                    lineCount++;
                    line = reader.readLine();
                }

                if(reader!=null){
                    reader.close();
                }
			}
			catch (Exception e) {
				System.out.println("Failed to count file lines." + e.getMessage());
				System.exit(0);
			}
			
			nLineCount = (int) (lineCount / mapperCount);
			nLineCount = (int) (Math.floor((nLineCount + 7) / 14) * 14);
		}
		
		int iterations = 0;
        if(itersStr != null) {
        	try {
        		iterations = Integer.parseInt(itersStr);
        	}
        	catch(NumberFormatException e) {
        		System.out.println("Failed to read iteration count.");
        		System.exit(0);
        	}
        	
        	if(iterations < 1) {
        		System.out.println("Insufficient number of iterations.");
                System.exit(0);
        	}
        }
        
        if(dateStr != null) {
        	try {
        		dateStr = Long.toString(utils.ISO8601.toTimeMS(dateStr));
        	}
        	catch(ParseException e) {
        		System.out.println("Failed to parse date provided.");
        		System.exit(0);
        	}
        }
		
		// Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf(), "PageRankJob");
		
		// Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(PageRank.class);
		
		// Set mapper and reducer classes
		job.setMapperClass(ArticleDateMapper.class);
		job.setReducerClass(ArticleDateReducer.class);
		
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", nLineCount);
		//job.getConfiguration().set("mapreduce.framework.name", "local"); <-- to run locally
		//job.getConfiguration().set("fs.defaultFS","file:///"); <-- to run locally
		job.getConfiguration().set("dateLimit", dateStr); //send the max date to the mapper
		
		// Set input and output format
		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set input and output paths; remember, these will be HDFS paths or URLs
        NLineInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "_temp0"));

		// Set other misc configuration parameters (#reducer tasks
		job.setNumReduceTasks(10);

		// Finally, submit the job to the cluster and wait for it to complete
		job.waitForCompletion(true);
		
		
		Job job2;
		boolean succeeded = false;
		for (int i = 0; i < iterations; i++) {
			Path currentOutPath = new Path(outputPath + "_temp" + Integer.toString(i+1));
			job2 = Job.getInstance(getConf(), "PageRankJob");
			job2.setJarByClass(PageRank.class);
			
			job2.setMapperClass(PageRankCalculationMapper.class);
			if(i == iterations - 1) {
				job2.getConfiguration().set("lastIter", "1");
				currentOutPath = new Path(outputPath);
			}
			job2.setReducerClass(PageRankCalculationReducer.class);
			
			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			//job2.getConfiguration().set("mapreduce.framework.name", "local"); <-- to run locally
			//job2.getConfiguration().set("fs.defaultFS","file:///"); <-- to run locally
			job2.setNumReduceTasks(30);
			KeyValueTextInputFormat.addInputPath(job2, new Path(outputPath + "_temp" + Integer.toString(i)));
			FileOutputFormat.setOutputPath(job2, currentOutPath);
			// Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
			

			// Finally, submit the job to the cluster and wait for it to complete;
			succeeded = job2.waitForCompletion(true);

			if (!succeeded) {
				// The program encountered an error before completing the loop; report it and/or take appropriate action
				System.err.println("It broke.");
				break;
			}
		}
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
}
