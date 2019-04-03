package com.smings.mapreduce.census;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CensusMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	
	/* (non-Javadoc)
	 * Key & values will become the input to the reducer class
	 * Context = helps interacting with the mapreduce hadoop ecosystem
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString(); //extract the single line of input to us
		String[] data = line.split(","); // data is now the array of 
		// 39, State-gov, 77516, Bachelors, 13, Never-married, Adm-clerical, Not-in-family, White, Male, 2174, 0, 40, United-States, <=50K
		
		try {
		 String maritalStatus =	data[5];
		// String income = data[data.length - 1]; //last column is the income
		 Double hrsPerWeek = Double.parseDouble(data[12]);
		 context.write(new Text(maritalStatus), new DoubleWritable(hrsPerWeek)); // this is our output per line of input
			
		} catch (Exception e) {
			System.out.println("Kuch gadbad hai!!!");
			e.printStackTrace();
		}
		
	}
	
}
