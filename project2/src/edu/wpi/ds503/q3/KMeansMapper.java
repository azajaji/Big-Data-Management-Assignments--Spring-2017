package edu.wpi.ds503.q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class KMeansMapper

		extends Mapper<LongWritable, PointWritable, PointWritable, PointWritable>{

	private static final Log LOG_JOB = LogFactory.getLog(KMeansMapper.class);

	private ArrayList<PointWritable> prevClusters = new ArrayList<PointWritable>();
	private ArrayList<PointWritable> newClusters = new ArrayList<PointWritable>();

	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration ();
		FileSystem dfs = FileSystem.get (conf);

		Path src = new Path (conf.get ("cluster_input"));
		FSDataInputStream fs = dfs.open (src);
		BufferedReader myReader = new BufferedReader (new InputStreamReader (fs));

		String cur_line = myReader.readLine ();
		while (cur_line != null) {

			PointWritable p = new PointWritable();
			PointWritable p_copy = new PointWritable();
			try {
				Scanner reader  = new Scanner (new StringReader(cur_line));
				int x = reader.nextInt();
				int y = reader.nextInt();

				p.set(x, y);
				p_copy.set(x, y);

				prevClusters.add(p);
				newClusters.add(p_copy);

			} catch (Exception e) {
			}
			cur_line = myReader.readLine();
		}
	}

	public void map(LongWritable key, PointWritable value, Context context) throws IOException, InterruptedException {
		try
		{
		if (value == null ) return;
			int xx = value.getx().get();
			int yy = value.gety().get();
			xx++;
			yy++;

		} catch (Exception e) {
			return;
		}

		LOG_JOB.info("MAP");

		for (int j = 0; j < prevClusters.size(); j++){
			LOG_JOB.info(prevClusters.get(j).getx().toString() + " , " + prevClusters.get(j).gety().toString() );
		}
    	int min_dist = (int) 9e99;
		int selected_cluster_index = -1;

		int x = value.getx().get();
		int y = value.gety().get();

		for (int i = 0; i < prevClusters.size (); i++) {
			PointWritable cluster_i = prevClusters.get(i);
			int centroid_x = cluster_i.getx().get();
			int centroid_y = cluster_i.gety().get();
			int distance = (int) (Math.pow( x-centroid_x,2)+ Math.pow( y-centroid_y,2));
			if (distance < min_dist) {
				min_dist = distance;
				selected_cluster_index = i;
			}
		}

		PointWritable selected_cluster = prevClusters.get(selected_cluster_index);
		context.write(selected_cluster, value);
	}

}