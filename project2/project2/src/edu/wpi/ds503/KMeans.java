package edu.wpi.ds503;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import edu.wpi.ds503.PointFileInputFormat;
import edu.wpi.ds503.PointWritable;



import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class KMeans {
	private static final Log LOG = LogFactory.getLog(KMeans.class);


	
	
	private static ArrayList<PointWritable> kseeds = new ArrayList<PointWritable>();
	
	
	// this method to initialize random points from input file
	private static void InitKSeed(String path, int k, Configuration conf) throws Exception {
		
		FileSystem dfs = FileSystem.get (conf);
	    FSDataInputStream fs = dfs.open (new Path (path));
	    BufferedReader br = new BufferedReader (new InputStreamReader (fs));
	    
	    
		//FileInputStream fs= new FileInputStream(path);
		//BufferedReader br = new BufferedReader(new InputStreamReader(fs));
		
		try {
			

			ArrayList<String> array = new ArrayList<>();
			String line;
			while((line = br.readLine()) != null)
			  array.add(br.readLine());
			Random rand = new Random();

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
				int x = reader.nextInt();
				int y = reader.nextInt();
				
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
		
	}
	
	
	// Main Run
	// ARGS: k ,  kseed_file ,  input ,  output
	public static void main(String[] args) throws Exception {
		Integer k = Integer.parseInt(args[0]);
		
		
		String unique_run = String.valueOf(System.nanoTime());
		
		Configuration conf = new Configuration();
		
		InitKSeed(args[1], k, conf);

		  
		
		for (int iteration=0; iteration < 10; iteration++){
			
			String input_fname = "/"+unique_run+"/clusters_input_"+iteration ; 
			
			if (iteration == 0) {
				org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(conf);
				
				Path path_clusters_input = new Path(input_fname+"/file");
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
		} else {
			
			
			org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get (conf); 
		      Path path = new Path (input_fname); 
		      FileStatus fstatus[] = hdfs.listStatus (path);
		      
		      int count = 0;
		      for (FileStatus f: fstatus) {
		        
		        if (f.getPath().toUri().getPath().contains ("/_"))
		          continue;
		        
		        count++;
		        conf.set ("cluster_input", f.getPath().toUri().getPath());
		      } 
		      
		      if (count != 1){
		    	  //error
		      }
			
		}
			
		String output_fname = "/"+unique_run+"/clusters_input_"+(iteration+1); 
			

		Job job = new Job(conf, "kmeans");
	

		job.setJarByClass(KMeans.class);
		job.setMapperClass(KMeansMapper.class);
		job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);

		job.setInputFormatClass (PointFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[2]));

		job.setMapOutputKeyClass(PointWritable.class);
		job.setMapOutputValueClass(PointWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(output_fname));
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true) ;
		}
		
		
	}


}
