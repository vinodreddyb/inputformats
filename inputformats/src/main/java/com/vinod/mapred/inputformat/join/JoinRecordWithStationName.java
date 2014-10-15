package com.vinod.mapred.inputformat.join;

// cc JoinRecordWithStationName Application to join weather records with station names

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

import com.vinod.mapred.inputformat.util.TextPair;

// vv JoinRecordWithStationName
public class JoinRecordWithStationName extends Configured implements Tool {
  
  public static class KeyPartitioner extends Partitioner<TextPair, Text> {
    @Override
    public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
      return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
   /* if (args.length != 3) {
      JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
      return -1;
    }*/
    
    Job job = new Job(getConf(), "Join weather records with station names");
    job.setJarByClass(getClass());
    
    Path ncdcInputPath = new Path("/home/vinod/git/inputformat/inputformats/src/main/resources/join/sample.txt");
    Path stationInputPath = new Path("/home/vinod/git/inputformat/inputformats/src/main/resources/join/stations-fixed-width.txt");
    Path outputPath = new Path("/home/vinod/work/join");
    outputPath.getFileSystem(getConf()).delete(outputPath, true);
    
    MultipleInputs.addInputPath(job, ncdcInputPath,
        TextInputFormat.class, JoinRecordMapper.class);
    MultipleInputs.addInputPath(job, stationInputPath,
        TextInputFormat.class, JoinStationMapper.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    /*[*/job.setPartitionerClass(KeyPartitioner.class);
    job.setGroupingComparatorClass(TextPair.FirstComparator.class);/*]*/
    
    job.setMapOutputKeyClass(TextPair.class);
    
    job.setReducerClass(JoinReducer.class);

    job.setOutputKeyClass(Text.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(),new JoinRecordWithStationName(), args);
    System.exit(exitCode);
  }
}
// ^^ JoinRecordWithStationName
