package com.smings.mapreduce;

import java.time.LocalDateTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

public class ViewCount extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new ViewCount(), args);
        System.exit(exitcode);		
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		Job job = Job.getInstance(conf);
		job.setJobName("ViewCountJob");
		job.setJarByClass(ViewCount.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setNumReduceTasks(2);
		job.setCombinerClass(MyReducer.class);
		
		Path inputFilePath = new Path("data/input");
		
		String string = LocalDateTime.now().toString();
		System.out.println("Date String = " + string);
		
		Path outputFilePath = new Path("data/output"+ string );
		
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
