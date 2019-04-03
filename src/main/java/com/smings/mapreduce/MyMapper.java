package com.smings.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] row = value.toString().split("\t");
		context.write(new Text(row[2]), new IntWritable(1));
		
	}
	
}
