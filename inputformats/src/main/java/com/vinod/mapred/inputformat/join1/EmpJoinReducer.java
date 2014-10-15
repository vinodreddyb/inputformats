package com.vinod.mapred.inputformat.join1;
// cc JoinReducer Reducer for joining tagged station records with tagged weather records
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.vinod.mapred.inputformat.avro.util.LongPair;

// vv JoinReducer
public class EmpJoinReducer extends Reducer<LongPair, Text, LongWritable, Text> {

	
  @Override
  protected void reduce(LongPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  
    Iterator<Text> iter = values.iterator();
    
    Text emp = new Text(iter.next());
    System.out.println(emp.toString());
    while (iter.hasNext()) {
      Text record = iter.next();
      Text outValue = new Text(emp.toString() + "\t" + record.toString());
      context.write(key.getFirst(), outValue);
    }
  }
}
// ^^ JoinReducer