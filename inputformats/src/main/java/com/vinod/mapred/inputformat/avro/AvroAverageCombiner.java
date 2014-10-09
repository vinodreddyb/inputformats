package com.vinod.mapred.inputformat.avro;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


import com.vinod.mapred.inputformat.avro.util.IntPair;

public class AvroAverageCombiner extends 
Reducer<IntWritable, IntPair, IntWritable, IntPair> {
	IntPair p_sum_count = new IntPair();
	Integer p_sum = new Integer(0);
	Integer p_count = new Integer(0);
	protected void reduce(IntWritable key, Iterable<IntPair> values, Context context) 
			throws IOException, InterruptedException {
		p_sum = 0;
		p_count = 0;
		for (IntPair value : values) {
			p_sum += value.getFirstInt();
			p_count += value.getSecondInt();
		}
		p_sum_count.set(new IntWritable(p_sum), new IntWritable(p_count));
		context.write(key, p_sum_count);
	}
}
