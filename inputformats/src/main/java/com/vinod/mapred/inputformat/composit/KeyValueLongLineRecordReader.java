package com.vinod.mapred.inputformat.composit;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class KeyValueLongLineRecordReader extends
		RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory
			.getLog(KeyValueLongLineRecordReader.class);
	public static final String FIXED_RECORD_LENGTH = "mapreduce.input.fixedlengthinputformat.record.length";
	// the start point of our split
	private long splitStart;

	// the end point in our split
	private long splitEnd;

	// our current position in the split
	private long currentPosition;

	// the length of a record
	private String split;

	// reference to the input stream
	private FSDataInputStream fileInputStream;

	

	// reference to our FileSplit
	private FileSplit fileSplit;

	// our record key (byte position)
	private LongWritable recordKey = null;

	// the record value
	private Text recordValue = null;

	private LineReader reader;

	
	

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		// the file input fileSplit
		this.fileSplit = (FileSplit) inputSplit;

		// the byte position this fileSplit starts at within the splitEnd file
		splitStart = fileSplit.getStart();

		// splitEnd byte marker that the fileSplit ends at within the splitEnd
		// file
		splitEnd = splitStart + fileSplit.getLength();

		// log some debug info
		LOG.info("FixedLengthRecordReader: SPLIT START=" + splitStart
				+ " SPLIT END=" + splitEnd + " SPLIT LENGTH="
				+ fileSplit.getLength());

		// the actual file we will be reading from
		Path file = fileSplit.getPath();

		// job configuration
		Configuration job = context.getConfiguration();

		// check to see if compressed....
		CompressionCodec codec = new CompressionCodecFactory(job)
				.getCodec(file);
		if (codec != null) {
			throw new IOException(
					"FixedLengthRecordReader does not support reading compressed files");
		}

		// THE JAR COMPILED AGAINST 0.20.1 does not contain a version of
		// FileInputFormat with these constants (but they exist in trunk)
		// uncomment the below, then comment or discard the line above
		// inputByteCounter =
		// ((MapContext)context).getCounter(FileInputFormat.COUNTER_GROUP,
		// FileInputFormat.BYTES_READ);

		// get the filesystem
		final FileSystem fs = file.getFileSystem(job);

		// open the File
		fileInputStream = fs.open(file, (64 * 1024));

		// seek to the splitStart position
		fileInputStream.seek(splitStart);

		// set our current position
		this.currentPosition = splitStart;
		this.split = getSpiltChar(job);

		reader = new LineReader(fileInputStream);

	}

	private String getSpiltChar(Configuration config) throws IOException {
		String splitChar = config.get("key.value.separator.in.input.line", ",");

		// this would be an error
		if (splitChar == null) {
			throw new IOException(
					"FixedLengthInputFormat requires the Configuration property:"
							+ FIXED_RECORD_LENGTH
							+ " to"
							+ " be set to something > 0. Currently the value is 0 (zero)");
		}

		return splitChar;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (recordKey == null) {
			recordKey = new LongWritable();
		}

		// the Key is always the position the record starts at
		recordKey.set(currentPosition);

		// the recordValue to place the record text in
		if (recordValue == null) {
			recordValue = new Text();
		} else {
			recordValue.clear();
		}
		int newSize = 0;

		// if the currentPosition is less than the split end..
		if (currentPosition < splitEnd) {
			newSize = reader.readLine(recordValue);
			String str = new String(recordValue.getBytes(), "UTF-8");
			System.out.println("String-------------->" + str);
			String[] splitLine = str.split(split);
			System.out.println("String split-------------->" + splitLine[0]);
			recordKey.set(Long.parseLong(splitLine[0]));
			currentPosition = currentPosition + newSize;
		}
		if (newSize == 0) {
			recordKey = null;
			recordValue = null;
			return false;
		} else {
			return true;
		}

		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return recordKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return recordValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (splitStart == splitEnd) {
		    return 0;
		  }
		  return Math.min(1.0f, (currentPosition - splitStart) / (float) (splitEnd - splitStart));
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
