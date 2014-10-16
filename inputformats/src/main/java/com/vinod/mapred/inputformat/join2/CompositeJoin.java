package com.vinod.mapred.inputformat.join2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.vinod.mapred.inputformat.join1.KeyValueLongInputFormat;

/**
yarn jar $PLAY_AREA/HadoopSamples.jar mr.joins.CompositeJoin <in1> <in2> <out>
yarn jar $PLAY_AREA/HadoopSamples.jar mr.joins.CompositeJoin examples_input/compositeJoin/data1/ examples_input/compositeJoin/data2/ /training/playArea/compositeJoin/
 */
public class CompositeJoin extends Configured implements Tool{
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());        
        job.setJarByClass(getClass());
        
       
        Path emp = new Path(
				"/home/vinod/git/inputformats/inputformats/src/main/resources/join/emp.txt");
		Path sal = new Path(
				"/home/vinod/git/inputformats/inputformats/src/main/resources/join/sal.txt");
		Path outputPath = new Path("/home/vinod/work/output/empjoin1");
		outputPath.getFileSystem(getConf()).delete(outputPath, true);
        
        job.setInputFormatClass(CompositeInputFormat.class);
        job.setMapperClass(CompositeJoinMapper.class);
        job.getConfiguration().set(CompositeInputFormat.JOIN_EXPR, 
                CompositeInputFormat.compose("inner", KeyValueLongInputFormat.class, emp, sal));
        job.setNumReduceTasks(0);
        TextOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),new  CompositeJoin(), args);
        System.exit(exitCode);
    }
}
