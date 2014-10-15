package com.vinod.mapred.inputformat.join;
// cc JoinStationMapper Mapper for tagging station records for a reduce-side join
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import com.vinod.mapred.inputformat.util.NcdcStationMetadataParser;
import com.vinod.mapred.inputformat.util.TextPair;

// vv JoinStationMapper
public class JoinStationMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
 

  NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    if (parser.parse(value)) {
      context.write(new TextPair(parser.getStationId(), "0"),
          new Text(parser.getStationName()));
    }
  }
}
// ^^ JoinStationMapper