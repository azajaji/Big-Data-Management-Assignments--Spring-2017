package edu.wpi.ds503.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yousef fadila on 06/02/2017.
 */


public class Query4 {
    private static final String CUSTOMER_PATH = "customersPath";
    public static class Query4Data implements Writable {

        private IntWritable custCount = new IntWritable();
        private FloatWritable trasTotal = new FloatWritable();

        public IntWritable getCustCount() {
            return custCount;
        }

        public FloatWritable getTrasTotal() {
            return trasTotal;
        }

        public Query4Data set(IntWritable custCount, FloatWritable trasTotal)
        {
            this.custCount = custCount;
            this.trasTotal = trasTotal;
            return this;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            custCount.write(dataOutput);
            trasTotal.write(dataOutput);

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            custCount.readFields(dataInput);
            trasTotal.readFields(dataInput);
        }
    }

    public static class  Query4Mapper
            extends Mapper<Object, Text, IntWritable, Query4Data> {


        private final static int CUST_CUSTOMER_ID = 0;
        private final static int CUST_CONTRY = 3;

        private final static int TRANS_CUSTOMER_ID  = 1;
        private final static int TRANS_TOTAL = 2;
        private Map<Integer, Integer> custToCountryMap = new HashMap<>();
        private int countryCustomers[] = new int[10];

            protected void setup(Context context) throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                String customerPath = conf.get(CUSTOMER_PATH);
                try {
                    Path pt = new Path(customerPath);
                    FileSystem fs = FileSystem.get(new Configuration());
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                    String line;
                    line = br.readLine();
                    while (line != null) {
                        String[] fields = line.toString().split(",");
                        int customerId=Integer.parseInt(fields[CUST_CUSTOMER_ID]);
                        int countryCode=Integer.parseInt(fields[CUST_CONTRY]);
                        custToCountryMap.put(customerId, countryCode);
                        // we already read all customers to make the map, the cost of counting the customer is negligible in both space and time
                        countryCustomers[countryCode - 1] = countryCustomers[countryCode - 1] + 1 ;
                        line = br.readLine();
                    }

                } catch (Exception e) {
                    throw new IOException("UNEXPECTED,error",e);
                }
            }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int customerId = Integer.parseInt(fields[TRANS_CUSTOMER_ID]);
            float transTotal = Float.parseFloat(fields[TRANS_TOTAL]);

            Integer country = custToCountryMap.get(customerId);
            if (country == null) {
                throw new IOException("UNEXPECTED, customer is not fount");
            }

            IntWritable countryIntWritable = new IntWritable(country);
            FloatWritable totalFloatWritable = new FloatWritable(transTotal);
            // we submit the same totalCustomersPerCountry for each transaction, this is redundant !write  it with flag and send it only once to reducer.
            IntWritable totalCustomersPerCountry = new IntWritable(countryCustomers[country - 1]);

            context.write(countryIntWritable, new Query4Data().set(totalCustomersPerCountry,totalFloatWritable));
        }
    }


    public static class Query4Reducer
            extends Reducer<IntWritable, Query4Data, NullWritable, Text> {

        public void reduce(IntWritable key, Iterable<Query4Data> values, Context context) throws IOException, InterruptedException {
            float min = Float.MAX_VALUE;
            float max = Float.MIN_VALUE;
            int customerNumber = 0;

            for (Query4Data val : values) {
                float total = val.getTrasTotal().get();
                min = total < min? total : min;
                max = total > max? total : max;
                //TODO put next line out of the loop.
                customerNumber = val.getCustCount().get();
            }

            Text result = new Text(key.get() + "," + customerNumber + "," + min + "," + max);
            context.write(NullWritable.get(), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Project1, Query4");
        job.setJarByClass(Query4.class);
        job.setMapperClass(Query4.Query4Mapper.class);
        job.setReducerClass(Query4.Query4Reducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Query4Data.class);
        
        job.getConfiguration().set(CUSTOMER_PATH,args[0]);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

  }
