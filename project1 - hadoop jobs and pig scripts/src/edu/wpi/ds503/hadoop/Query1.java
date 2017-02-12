package edu.wpi.ds503.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yousef fadila on 06/02/2017.
 */


public class Query1 {
    // The output is customerId, customerCountryCode
    public static class CustomersQuery1Mapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final static int CUSTOMER_ID = 0;
        private final static int COUNTRY_CODE = 3;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int customerId=Integer.parseInt(fields[CUSTOMER_ID]);
            int countryCode=Integer.parseInt(fields[COUNTRY_CODE]);

            if(countryCode>=2 && countryCode<=6){
                IntWritable customerIdIntWriteable = new IntWritable(customerId);
                IntWritable countryCodeIntWriteable = new IntWritable(countryCode);
                context.write(customerIdIntWriteable, countryCodeIntWriteable);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Project1-Query1");
        job.setJarByClass(Query1.class);

        job.setMapperClass(Query1.CustomersQuery1Mapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

  }
