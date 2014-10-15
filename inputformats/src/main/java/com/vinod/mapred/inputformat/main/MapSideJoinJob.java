package com.vinod.mapred.inputformat.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vinod.mapred.inputformat.composit.KeyValueLongInputFormat;
import com.vinod.mapred.inputformat.composit.EmployeeJoinMapper;

public class MapSideJoinJob extends Configured implements Tool{

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {

		this.conf = conf;
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(MapSideJoinJob.class);
		job.setJobName("Mapside join input");
		Path inPath = new Path("/home/vinod/git/inputformat/inputformats/src/main/resources/join/emp.txt");
		Path outPath = new Path("/home/vinod/work/output/join");

		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		
		conf.set("key.value.separator.in.input.line", ",");
		job.setInputFormatClass(KeyValueLongInputFormat.class);
		job.setMapperClass(EmployeeJoinMapper.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int result = ToolRunner.run(conf,new MapSideJoinJob(), args);
		System.exit(result);
	}
}
