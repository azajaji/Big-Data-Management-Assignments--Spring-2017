package edu.wpi.ds503;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



public class JSONFileInputFormat extends FileInputFormat<LongWritable,Text>{
	 
 

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,
			org.apache.hadoop.mapreduce.TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new JSONLineRecordReader();
	}
}

