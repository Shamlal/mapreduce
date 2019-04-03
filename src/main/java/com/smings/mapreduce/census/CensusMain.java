package com.smings.mapreduce.census;

import java.time.LocalDateTime;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;


/*
 * This class sets up a job to run MapReduce
 * Configured class provides the configuration object, used to specify 
 * parameters to the MapReduce
 * Tool interface allows us to use parameters from the command line
 */
public class CensusMain extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("averages");
		job.setJarByClass(CensusMain.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// we also need to specify the output datatypes of the Reduce phase, but if they are the same then no need to specify
		
		job.setMapperClass(CensusMapper.class);
		job.setReducerClass(CensusReducer.class);
		
		String string = LocalDateTime.now().toString();
		System.out.println("Date String = " + string);
		
		Path inputFilePath = new Path("data/input_census/census.txt");
		Path outputFilePath = new Path("data/output_census_" + string);
		
		// * important there are 2 types of FileInputFormats..
		// the other one does not take job but jobConf.. which is not here.
		
		FileInputFormat.addInputPath(job , inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CensusMain(), args);
		System.exit(exitCode);
	}
	
	

}
