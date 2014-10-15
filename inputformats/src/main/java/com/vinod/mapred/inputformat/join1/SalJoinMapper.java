package com.vinod.mapred.inputformat.join1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.vinod.mapred.inputformat.avro.util.LongPair;

public class SalJoinMapper extends Mapper<LongWritable, Text, LongPair, Text>{

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		String valString = value.toString();
        int index = valString.indexOf(',');
		String val = valString.substring(index+1);
		context.write(new LongPair(key.get(),1L), new Text(val));
	}
}
