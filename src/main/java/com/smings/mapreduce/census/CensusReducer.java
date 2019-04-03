package com.smings.mapreduce.census;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * @author shamayla
 * Text, DoubleWritable - Input key and value to the Redue phase - should match o/p of Map phase
 * Text, DoubleWritable - final output results from Reduce phase - marital status and average number of hours per week
 */
public class CensusReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	/*
	 * final Text key, final Iterable<DoubleWritable> values - it receives the list of all the values associated with a single key
	 * E.g. = marital status, List<numbers> hours.
	 * 
	 */
	@Override
	public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
		Double sum = 0.0;
		Integer count = 0;
		for (DoubleWritable doubleWritable : values) {
			sum += doubleWritable.get();
			count++;
		}
		Double ratio = sum/count;
		context.write(key, new DoubleWritable(ratio));
	}
}
