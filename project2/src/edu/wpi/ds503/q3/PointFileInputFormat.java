package edu.wpi.ds503.q3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class PointFileInputFormat extends FileInputFormat<LongWritable, PointWritable>{

    @Override
    public RecordReader<LongWritable, PointWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new PointRecordReader();
    }

}