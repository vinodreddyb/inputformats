package com.vinod.mapred.inputformat.composit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.vinod.mapred.inputformat.util.TextPair;

public class EmployeeJoinMapper extends Mapper<LongWritable, Text, TextPair, Text>{

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		
		System.out.println(key);
		System.out.println(value.toString());
		
	}
}
