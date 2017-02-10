package edu.wpi.ds503.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yousef fadila on 06/02/2017.
 */


public class Query2 {
    public static class CustomerQuery2Data implements WritableComparable<CustomerQuery2Data> {

        private IntWritable customerId = new IntWritable();
        private IntWritable count = new IntWritable();
        private FloatWritable sum = new FloatWritable();

        public IntWritable getCount() {
            return count;
        }

        public FloatWritable getSum() {
            return sum;
        }

        public IntWritable getCustomerId() {
            return customerId;
        }

        //Setter method to set the values of WebLogWritable object
        public CustomerQuery2Data set(IntWritable customerId, IntWritable count, FloatWritable sum)
        {
            this.customerId = customerId;
            this.count = count;
            this.sum = sum;
            return this;
        }

        @Override
        public int compareTo(CustomerQuery2Data o) {
            // we won't use this type as key, but as value. compareTo is not important for this usage.
            return customerId.compareTo(o.customerId);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            customerId.write(dataOutput);
            count.write(dataOutput);
            sum.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            customerId.readFields(dataInput);
            count.readFields(dataInput);
            sum.readFields(dataInput);
        }
    }


        public static class CustomersQuery2Mapper
            extends Mapper<Object, Text, IntWritable, CustomerQuery2Data> {

        private final static int CUST_ID = 1;
        private final static int TRANS_TOTAL = 2;
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int customerId=Integer.parseInt(fields[CUST_ID]);
            float transTotal=Float.parseFloat(fields[TRANS_TOTAL]);

            IntWritable customerIdIntWriteable = new IntWritable(customerId);
            FloatWritable totalFloatWriteable = new FloatWritable(transTotal);
            context.write(customerIdIntWriteable, new CustomerQuery2Data().set(customerIdIntWriteable, one,totalFloatWriteable));
        }
    }

    public static class CustomersQuery2Combiner
            extends Reducer<IntWritable,CustomerQuery2Data,IntWritable,CustomerQuery2Data> {

        public void reduce(IntWritable key, Iterable<CustomerQuery2Data> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;

            for (CustomerQuery2Data val : values) {
                float total = val.getSum().get();
                sum += total;
                count +=val.getCount().get();
            }

            context.write(key, new CustomerQuery2Data().set(key,new IntWritable(count), new FloatWritable(sum)));
        }
    }

    public static class CustomersQuery2Reducer
            extends Reducer<IntWritable,CustomerQuery2Data,NullWritable,Text> {

        public void reduce(IntWritable key, Iterable<CustomerQuery2Data> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;

            for (CustomerQuery2Data val : values) {
                float total = val.getSum().get();
                sum += total;
                count +=val.getCount().get();
            }

            Text result = new Text(key.get() + "," + count + "," + sum);
            context.write(NullWritable.get(), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Project1, Query2");
        job.setJarByClass(Query2.class);
        job.setMapperClass(Query2.CustomersQuery2Mapper.class);
        job.setCombinerClass(Query2.CustomersQuery2Combiner.class);
        job.setReducerClass(Query2.CustomersQuery2Reducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Query2.CustomerQuery2Data.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

  }
