package edu.wpi.ds503;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public  class KMeansCombiner  extends Reducer<PointWritable, PointWritable,PointWritable,PointWritable> {

	public void reduce(PointWritable centroidid, Iterable<PointWritable> points, 
			Context context
			) throws IOException, InterruptedException {
		
		
		String key = centroidid.toString();
		
		
		
		int num = 0;
		int centerx=0;
		int centery=0;
		for (PointWritable point : points) {
			

			num++;
			IntWritable X = point.getx();
			IntWritable Y = point.gety();
			int x = X.get();
			int y = Y.get();
			
		
			centerx += x;
			centery += y;
		}
		int sumx = centerx;
		int sumy = centery;
		
		centerx = centerx/num;
		centery = centery/num;
		
		PointWritable newcentroid = new PointWritable(centerx,centery);
		
		
		context.write(centroidid , newcentroid);
		
		
		
	}
	
	
	
}