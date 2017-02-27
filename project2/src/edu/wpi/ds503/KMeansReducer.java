package edu.wpi.ds503;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;


public  class KMeansReducer  extends Reducer<PointWritable, PointWritable,NullWritable,Text> {

	public enum UNCHANGED_COUNTER {
		VALUE
	}

	private ArrayList<PointWritable> clusters  = new ArrayList<PointWritable>();

	Reporter reporter;

	public void reduce(PointWritable centroidid, Iterable<PointWritable> points,Context context	) throws IOException, InterruptedException {

		int sumx = 0;
		int sumy = 0;
		int num = 0;
		for (PointWritable point : points) {
			sumx += point.getx().get();
			sumy += point.gety().get();
			num ++;
		}

		int x = sumx / num;
		int y = sumy / num;

		PointWritable result_point = new PointWritable(x,y);
		//Text result = new Text(String.valueOf(result_point.getx().get()) + " " + String.valueOf(result_point.gety().get()) + " Num: "+num  + ", sumx: " + sumx + ", sumy: " + sumy);
		Text result = new Text(String.valueOf(result_point.getx().get()) + " " + String.valueOf(result_point.gety().get()));

		clusters.add(result_point);

		context.write( NullWritable.get(), result);
	}

	protected void cleanup (Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration ();
		FileSystem dfs = FileSystem.get (conf);
		String file_path = conf.get ("cluster_input");
		Pattern pattern = Pattern.compile("clusters_input_(\\d+)");
		Matcher matcher = pattern.matcher(file_path);
		matcher.find();

		String str_iteration = matcher.group(1);

		int iteration = Integer.parseInt(str_iteration);

		if (iteration == 0) return;

		String compare_file = matcher.replaceAll("clusters_input_"+String.valueOf((iteration-1)));

		Path src = new Path (compare_file);
		FSDataInputStream fs = dfs.open (src);
		BufferedReader myReader = new BufferedReader (new InputStreamReader (fs));

		String cur_line = myReader.readLine ();

		int matches = 0;
		int count = 0;

		while (cur_line != null) {
			try {
				PointWritable p = new PointWritable();
				PointWritable p_copy = new PointWritable();

				Scanner reader  = new Scanner (new StringReader(cur_line));
				int x = reader.nextInt();
				int y = reader.nextInt();

				p.set(x, y);
				p_copy.set(x, y);

				for (PointWritable p_cur: clusters) {
					if (p_cur.equals(p) ) {
						matches++ ;
						break;
					}
				}
				count++;

				cur_line = myReader.readLine();
			} catch(Exception e) {
				break;
			}
		}

		if (count == matches) {
			context.write( NullWritable.get(), new Text("---NOTHING HAS CHANGED----"));
			context.getCounter("STATUS", "unchanged").increment(1);
		}
	}
}