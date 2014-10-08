package com.vinod.mapred.inputformat.main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vinod.mapred.inputformat.combine.CFInputFormat;
import com.vinod.mapred.inputformat.combine.EmployeeCombineMapper;


public class TestMain extends Configured implements Tool {

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
		job.setJarByClass(TestMain.class);
		job.setJobName("Combine input");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(CFInputFormat.class);
		job.setMapperClass(EmployeeCombineMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(EmployeeCombineMapper.class);
		// job.setReducerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		/*
		 * conf.set("fs.default.name", "hdfs://ubuntu:9000");
		 * conf.set("mapred.job.tracker", "ubuntu:9001");
		 */
		ToolRunner.run(conf, new TestMain(), args);
	}

}
