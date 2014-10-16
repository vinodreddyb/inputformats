package com.vinod.mapred.inputformat.join2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class CompositeJoinMapper extends Mapper<LongWritable, TupleWritable, Text, Text> {
	Text txtKey = new Text("");
	Text txtValue = new Text("");
    @Override
    protected void map(LongWritable key, TupleWritable value, Context context)
            throws IOException, InterruptedException {
    	txtKey.set(key.toString());
    	 
		String arrEmpAttributes[] = value.get(0).toString().split(",");
		String arrDeptAttributes[] = value.get(1).toString().split(",");

		txtValue.set(arrEmpAttributes[1].toString() + "\t"
				+ arrEmpAttributes[2].toString() + "\t"
				+ arrDeptAttributes[0].toString());

        context.write(txtKey, txtValue);
    }
}
