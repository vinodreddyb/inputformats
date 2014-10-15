package com.vinod.mapred.inputformat.avro.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class LongPair implements WritableComparable<LongPair> {
	private LongWritable first;
	private LongWritable second;

	public LongPair() {
		set(new LongWritable(), new LongWritable());
	}

	public LongPair(Long first, Long second) {
		set(new LongWritable(first), new LongWritable(second));
	}

	public void set(LongWritable first, LongWritable second) {
		this.first = first;
		this.second = second;
	}

	public LongWritable getFirst() {
		return first;
	}

	public Integer getFirstInt() {
		return new Integer(first.toString());
	}

	public Integer getSecondInt() {
		return new Integer(second.toString());
	}

	public LongWritable getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LongPair other = (LongPair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	@Override
	public int compareTo(LongPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}

	@Override
	public String toString() {
		return "LongPair [first=" + first + ", second=" + second + "]";
	}
	
}
