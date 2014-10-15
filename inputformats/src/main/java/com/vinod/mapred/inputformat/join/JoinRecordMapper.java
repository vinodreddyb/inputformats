package com.vinod.mapred.inputformat.join;
// cc JoinRecordMapper Mapper for tagging weather records for a reduce-side join
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.vinod.mapred.inputformat.util.NcdcRecordParser;
import com.vinod.mapred.inputformat.util.TextPair;

//vv JoinRecordMapper
public class JoinRecordMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    parser.parse(value);
    context.write(new TextPair(parser.getStationId(), "1"), value);
  }

}
//^^ JoinRecordMapper