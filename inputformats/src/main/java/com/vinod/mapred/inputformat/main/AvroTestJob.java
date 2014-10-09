package com.vinod.mapred.inputformat.main;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vinod.mapred.inputformat.avro.AvroAverageCombiner;
import com.vinod.mapred.inputformat.avro.AvroAverageMapper;
import com.vinod.mapred.inputformat.avro.AvroAverageReducer;
import com.vinod.mapred.inputformat.avro.util.IntPair;
import com.vinod.mapred.inputformat.avro.util.StudentMarks;

public class AvroTestJob extends Configured implements Tool{

	@Override
	public int run(String[] rawArgs) throws Exception {

		if (rawArgs.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Job job = new Job(super.getConf());
		job.setJarByClass(AvroTestJob.class);
		job.setJobName("Avro Average");
		
		String[] args = new GenericOptionsParser(rawArgs).getRemainingArgs();
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);

		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(super.getConf()).delete(outPath, true);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(AvroAverageMapper.class);
		AvroJob.setInputKeySchema(job, StudentMarks.getClassSchema());
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntPair.class);
		
		job.setCombinerClass(AvroAverageCombiner.class);
		
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(AvroAverageReducer.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
		AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.FLOAT));
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new AvroTestJob(), args);
		System.exit(result);
	}

}
