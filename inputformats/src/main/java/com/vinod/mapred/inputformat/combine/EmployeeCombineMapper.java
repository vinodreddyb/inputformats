package com.vinod.mapred.inputformat.combine;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeeCombineMapper extends
		Mapper<FileLineWritable, Text, Text, Text> {
	Text txtKey = new Text("");
	Text txtValue = new Text("");

	@Override
	protected void map(FileLineWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		if (value.toString().length() > 0) {
			String[] arrEmpAttributes = value.toString().split(":");
			System.out.println(value.toString());
			
			txtKey.set(arrEmpAttributes[0].toString());
			txtValue.set(arrEmpAttributes[2].toString() + "\t"
					+ arrEmpAttributes[3].toString());

			context.write(txtKey, txtValue);
		}
		
	}
}
