package com.vinod.mapred.inputformat.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vinod.mapred.inputformat.avro.util.LongPair;
import com.vinod.mapred.inputformat.join1.EmpJoinReducer;
import com.vinod.mapred.inputformat.join1.EmployeeJoinMapper;
import com.vinod.mapred.inputformat.join1.KeyValueLongInputFormat;
import com.vinod.mapred.inputformat.join1.SalJoinMapper;

/**
 * 
 * @author vinod
 *Emp,Sal map side join using multiple inputs
 */
public class MapSideJoinJob extends Configured implements Tool {

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {

		this.conf = conf;
	}

	public static class KeyPartitioner extends Partitioner<LongPair, Text> {
		@Override
		public int getPartition(/* [ */LongPair key/* ] */, Text value,
				int numPartitions) {
			return (/* [ */key.getFirst().hashCode()/* ] */& Integer.MAX_VALUE)
					% numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * if (args.length != 3) { JobBuilder.printUsage(this,
		 * "<ncdc input> <station input> <output>"); return -1; }
		 */

		//
		Job job = new Job(getConf(), "Join weather records with station names");
		job.setJarByClass(getClass());

		Path emp = new Path(
				"/home/vinod/git/inputformats/inputformats/src/main/resources/join/emp.txt");
		Path sal = new Path(
				"/home/vinod/git/inputformats/inputformats/src/main/resources/join/sal.txt");
		Path outputPath = new Path("/home/vinod/work/output/empjoin");
		outputPath.getFileSystem(getConf()).delete(outputPath, true);

		MultipleInputs.addInputPath(job, emp, KeyValueLongInputFormat.class,
				EmployeeJoinMapper.class);
		MultipleInputs.addInputPath(job, sal, KeyValueLongInputFormat.class,
				SalJoinMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		job.setPartitionerClass(KeyPartitioner.class);
		job.setMapOutputKeyClass(LongPair.class);

		job.setReducerClass(EmpJoinReducer.class);

		job.setOutputKeyClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int result = ToolRunner.run(conf, new MapSideJoinJob(), args);
		System.exit(result);
	}
}
