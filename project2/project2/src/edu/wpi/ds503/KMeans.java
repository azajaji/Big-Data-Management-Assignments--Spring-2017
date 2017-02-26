package edu.wpi.ds503;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.io.FileReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.wpi.ds503.PointFileInputFormat;
import edu.wpi.ds503.PointWritable;


import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FileSystem;




public class KMeans {
	private static final Log LOG = LogFactory.getLog(KMeans.class);

	public static class KmeansMapper 

	extends Mapper<LongWritable, PointWritable, Text, Text>{
		
		private static final Log LOG_JOB = LogFactory.getLog(KmeansMapper.class);
		
		private ArrayList<PointWritable> prevClusters = new ArrayList<PointWritable>();
		private ArrayList<PointWritable> newClusters = new ArrayList<PointWritable>();
		
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration ();
			FileSystem dfs = FileSystem.get (conf);
		    
		    if (conf.get ("cluster_input") == null) { 
		      throw new RuntimeException ("no cluster file!");
		    }
		    
		    Path src = new Path (conf.get ("cluster_input"));
		    FSDataInputStream fs = dfs.open (src);
		    BufferedReader myReader = new BufferedReader (new InputStreamReader (fs));
		    
		    String cur_line = myReader.readLine (); 
		    while (cur_line != null) {
		    	
		    	PointWritable p = new PointWritable();
		    	PointWritable p_copy = new PointWritable();
				
				Scanner reader  = new Scanner (new StringReader(cur_line));
				float x = reader.nextFloat();
				float y = reader.nextFloat();
				
				p.set(x, y);
				p_copy.set(x, y);
				
				prevClusters.add(p);
				newClusters.add(p_copy);
		    	
				
				cur_line = myReader.readLine();
		    }
		    
		    
		    
			

		}

		public void map(LongWritable key, PointWritable value, Context context
				) throws IOException, InterruptedException {
			
			
			try
			{ 
				
			
			if (value == null ) return;
			
			

			
			
			float xx = value.getx().get();
			float yy = value.gety().get();
			xx++;
			yy++;
			
			} catch (Exception e) {
				return;
			}
			
			LOG_JOB.info("MAP");
		    
		    for (int j = 0; j < prevClusters.size(); j++){
		    	LOG_JOB.info(prevClusters.get(j).getx().toString() + " , " + prevClusters.get(j).gety().toString() );
			}
		    
			
			float min_dist = (float) 9e99;
		    int selected_cluster_index = -1;
		    
		    float x = value.getx().get();
	    	float y = value.gety().get();
	    	
		    for (int i = 0; i < prevClusters.size (); i++) {
		    	PointWritable cluster_i = prevClusters.get(i);
		    	
		    	
		    	
		    	float centroid_x = cluster_i.getx().get();
		    	float centroid_y = cluster_i.gety().get();
		    	
		    	
		    	float distance = (float) (Math.pow( x-centroid_x,2)+ Math.pow( y-centroid_y,2));
		    	
		    	if (distance < min_dist) {
		    		min_dist = distance;
		    		selected_cluster_index = i;
		    	}
		      }
		    
		    
		    PointWritable selected_cluster = prevClusters.get(selected_cluster_index);
		    
		    LOG.info("point :"+ String.format("%f %f", x, y) + " goes to " + String.format("%f %f", selected_cluster.getx().get(), selected_cluster.gety().get()));

		    
			context.write(new Text(selected_cluster.toString()),new Text(value.toString())  );
		    //context.write(selected_cluster, value);
		}

	}

	public static class KmeansReducer 
	extends Reducer<Text, Text,Text,Text> {

		private HashMap<String,ArrayList<PointWritable>> clusters  = new HashMap<String,ArrayList<PointWritable>>();
		
		
		public void reduce(Text centroidid, Iterable<Text> points, 
				Context context
				) throws IOException, InterruptedException {
			
//			Scanner reader  = new Scanner (new StringReader(cluster_str.toString()));
//			float x = reader.nextFloat();
//			float y = reader.nextFloat();
//			PointWritable clusterid = new PointWritable();
//			clusterid.set(x, y);
			
			String key = centroidid.toString();
			
			if (!clusters.containsKey(key)) {
				clusters.put(key, new ArrayList<PointWritable>());
			}
			
			

//			for (PointWritable point : points) {
//				clusters.get(key).add(point);
//			}
			
			
			
			int num = 0;
			float centerx=0.0f;
			float centery=0.0f;
			for (Text point_str : points) {
				
				Scanner reader  = new Scanner (new StringReader(point_str.toString()));
				float xx = reader.nextFloat();
				float yy = reader.nextFloat();
				PointWritable point = new PointWritable();
				point.set(xx, yy);
				
				
				num++;
				FloatWritable X = point.getx();
				FloatWritable Y = point.gety();
				float x = X.get();
				float y = Y.get();
				
				if (Float.isNaN(x) || Float.isNaN(y)) {
					continue;
				}
				centerx += x;
				centery += y;
			}
			float sumx = centerx;
			float sumy = centery;
			
			centerx = centerx/num;
			centery = centery/num;
			
			LOG.info("new centroid:"+ String.valueOf(centerx) + " " + String.valueOf(centery)  );
			
			Text result = new Text(String.valueOf(centerx) + " " + String.valueOf(centery) + "," +  String.valueOf(sumx) + " " + String.valueOf(sumy) + "," + num);
			context.write(centroidid, result);
			
		}
		
		protected void cleanup (Context context) throws IOException, InterruptedException {
//			for ( ArrayList<PointWritable> c :  clusters.values() ) {
//				int num = 0;
//				float centerx=0.0f;
//				float centery=0.0f;
//				for (PointWritable point : c) {
//					num++;
//					FloatWritable X = point.getx();
//					FloatWritable Y = point.gety();
//					float x = X.get();
//					float y = Y.get();
//					centerx += x;
//					centery += y;
//				}
//				centerx = centerx/num;
//				centery = centery/num;
//				
//				LOG.info("new centroid:"+ String.format("%f %f", centerx, centery) );
//				
//				String preres = String.format("%f %f", centerx, centery);
//				Text result = new Text(preres);
//				//context.write(new Text(""), result);
//			}
		}
	}

	// ARGS: k ,  kseed_file ,  input ,  output
	public static void main(String[] args) throws Exception {
		Integer k = Integer.parseInt(args[0]);
		ArrayList<PointWritable> kseeds = new ArrayList<PointWritable>();
		
		
		Configuration conf = new Configuration();
		
		FileInputStream fs= new FileInputStream(args[1]);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs));
		
		try {

			ArrayList<String> array = new ArrayList<>();
			String line;
			while((line = br.readLine()) != null)
			  array.add(br.readLine());
			// variable so that it is not re-seeded every call.
			Random rand = new Random();

			// nextInt is exclusive. Should be good with output for array.
			for (int i = 0; i < k; i++) {
				int random_index= rand.nextInt(array.size());
				PointWritable p = new PointWritable();
				String point_str = array.get(random_index);
				if (point_str == null) {
					i--;
					continue;
				}
				array.remove(random_index);
				
				Scanner reader  = new Scanner (new StringReader(point_str));
				float x = reader.nextFloat();
				float y = reader.nextFloat();
				
				p.set(x, y);
				kseeds.add( p);
				
			}

			LOG.info("INFO::: KSEEDS");
			for (int j = 0; j < kseeds.size(); j++){
				LOG.info(kseeds.get(j).getx().toString() + " , " + kseeds.get(j).gety().toString() );
			}
		}  finally {
		    br.close();
		}
			
			
			
			
			
			org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(conf);
			
			//FSDataInputStream in = hdfs.open(new Path(""));
			String init_input_fname = String.format("clusters_input_0_%s", String.valueOf(System.nanoTime())) ; 
			
			Path path_clusters_input = new Path(init_input_fname);
		    FSDataOutputStream out = hdfs.create(path_clusters_input);
		    
		    
		    
		    try {
		    	for (int j = 0; j < kseeds.size(); j++){
		    		String kseed_line = kseeds.get(j).getx().toString() + " " + kseeds.get(j).gety().toString();
		    		out.writeBytes(kseed_line);
		    		out.writeBytes("\n");
		    	}
		    	
		    	conf.set ("cluster_input", path_clusters_input.toUri().getPath());
		    	
		    	LOG.info("CLUSTER INPUT SET ");
		    	LOG.info(path_clusters_input.toUri().getPath().toString());
		    	
		    } catch (IOException e) {
		      
		    } finally {
		      
		      out.close();
		    }
		    
		    
			
		

		Job job = new Job(conf, "kmeans");
		//Path toCache = new Path("/centers/centers.txt");
		//job.addCacheFile(toCache.toUri());
		//job.createSymlink();

		job.setJarByClass(KMeans.class);
		job.setMapperClass(KmeansMapper.class);
		job.setReducerClass(KmeansReducer.class);

		job.setInputFormatClass (PointFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[2]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
