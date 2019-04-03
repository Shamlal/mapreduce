package com.smings.nci.pda.mapreduce.distinct;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistinctValue extends Configured implements Tool{
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			context.write(new Text(values[1]), NullWritable.get());
		}
		
	}
	
	public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new DistinctValue(), args);
        System.exit(exitcode);
	}


	public int run(String[] arg0) throws Exception {
		
		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(DistinctValue.class);
		job.setJobName("DistinctValue");
		
		// input & output files 
		Path inputPath = new Path("data/distinct/input.txt");
		Path outputPath = new Path("data/distinct/output_"+ new Date().toString());
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		// output keys
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		return job.waitForCompletion(true)? 0 : 1;
	}

}