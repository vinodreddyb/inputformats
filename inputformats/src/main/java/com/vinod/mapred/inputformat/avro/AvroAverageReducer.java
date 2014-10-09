package com.vinod.mapred.inputformat.avro;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


import com.vinod.mapred.inputformat.avro.util.IntPair;

public  class AvroAverageReducer extends 
Reducer<IntWritable, IntPair, AvroKey<Integer>, AvroValue<Float>> {
	Integer f_sum = 0;
	Integer f_count = 0;
	
	protected void reduce(IntWritable key, Iterable<IntPair> values, Context context) 
			throws IOException, InterruptedException {
		f_sum = 0;
		f_count = 0;
		for (IntPair value : values) {
			f_sum += value.getFirstInt();
			f_count += value.getSecondInt();
		}
		Float average = (float)f_sum/f_count;
		Integer s_id = new Integer(key.toString());
		context.write(new AvroKey<Integer>(s_id), new AvroValue<Float>(average));
	}
}
