package com.vinod.mapred.inputformat.avro;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.vinod.mapred.inputformat.avro.util.IntPair;
import com.vinod.mapred.inputformat.avro.util.StudentMarks;



public class AvroAverageMapper extends
		Mapper<AvroKey<StudentMarks>, NullWritable, IntWritable, IntPair> {
	
	protected void map(AvroKey<StudentMarks> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		
		IntWritable s_id = new IntWritable(key.datum().getStudentId());
		IntPair marks_one = new IntPair(key.datum().getMarks(), 1);
		context.write(s_id, marks_one);
	}
} 