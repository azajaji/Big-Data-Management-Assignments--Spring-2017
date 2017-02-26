package edu.wpi.ds503;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class KMeansReducer  extends Reducer<PointWritable, PointWritable,NullWritable,Text> {

	private HashMap<String,ArrayList<PointWritable>> clusters  = new HashMap<String,ArrayList<PointWritable>>();
	
	
	public void reduce(PointWritable centroidid, PointWritable point, 
			Context context
			) throws IOException, InterruptedException {
		
		
		
		Text result = new Text(String.valueOf(point.getx().get()) + " " + String.valueOf(point.gety().get()) );

		context.write( NullWritable.get(), result);
		
	}
	
	
}