package edu.wpi.ds503.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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


public class Query5 {
    private static final String CUSTOMER_PATH = "customersPath";
    private static final String STEP2_PATH = "step2Path";

    public static class Step2Query5Data implements Writable {

        private IntWritable count = new IntWritable();

        private FloatWritable average = new FloatWritable();

        public FloatWritable getAverage() {
            return average;
        }

        public IntWritable getCount() {
            return count;
        }

        public Step2Query5Data set(IntWritable count, FloatWritable average)
        {
            this.count = count;
            this.average = average;
            return this;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            count.write(dataOutput);
            average.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            count.readFields(dataInput);
            average.readFields(dataInput);
        }
    }


    public static class Step2Query5Mapper
            extends Mapper<Object, Text, NullWritable, Step2Query5Data> {

        private final static int TRANS_NUM = 1;
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            // 19,105,55984.484 (query2 output is CustomerId, TotalTrans,Sum
            float transNum=Float.parseFloat(fields[TRANS_NUM]);
            FloatWritable transNumFloatWriteable = new FloatWritable(transNum);
            context.write(NullWritable.get(), new Step2Query5Data().set(one, transNumFloatWriteable));
        }
    }

    public static class Step2Query5Combiner
            extends Reducer<NullWritable,Step2Query5Data, NullWritable,Step2Query5Data> {

        public void reduce(NullWritable key, Iterable<Step2Query5Data> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;

            for (Step2Query5Data val : values) {
                float average = val.getAverage().get();
                int _count = val.getCount().get();

                sum += average * _count;
                count +=val.getCount().get();
            }

            float newAverage = sum/count;
            context.write(key, new Step2Query5Data().set(new IntWritable(count),
                    new FloatWritable(newAverage)));
        }
    }

    public static class Step2Query5Reducer
            extends Reducer<NullWritable,Step2Query5Data,NullWritable,Text> {

        public void reduce(NullWritable key, Iterable<Step2Query5Data> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;

            for (Step2Query5Data val : values) {
                float average = val.getAverage().get();
                int _count = val.getCount().get();

                sum += average * _count;
                count +=val.getCount().get();
            }

            float newAverage = sum/count;
            // print count for validation
            Text result = new Text(count + "," + newAverage);
            context.write(NullWritable.get(), result);
        }
    }

    public static class  Step3Query5Mapper
            extends Mapper<Object, Text, NullWritable, Text> {

        private final static int CUST_CUSTOMER_ID = 0;
        private final static int CUST_NAME = 1;
        private Map<Integer, String> custIdToNameMap = new HashMap<>();

        private float global_average;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String customerPath = conf.get(CUSTOMER_PATH);
            String step2OutputPath = conf.get(STEP2_PATH);
            try {
                Path pt = new Path(customerPath);
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] fields = line.toString().split(",");
                    int customerId=Integer.parseInt(fields[CUST_CUSTOMER_ID]);
                    custIdToNameMap.put(customerId, fields[CUST_NAME]);
                    line = br.readLine();
                }
                // read the one line of the putput of step 2. format COUNT,AVERAGE.
                pt= new Path(step2OutputPath);
                br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                line = br.readLine();
                String[] fields = line.toString().split(",");
                global_average = Float.parseFloat(fields[1]);
            } catch (Exception e) {
                throw new IOException("UNEXPECTED,error",e);
            }
        }
        private final static int TRANS_NUM = 1;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // 19,105,55984.484 (query2 output is CustomerId, TotalTrans,Sum
            int customerId=Integer.parseInt(fields[CUST_CUSTOMER_ID]);
            int transNum=Integer.parseInt(fields[TRANS_NUM]);

            if (transNum > global_average) {
                String name = custIdToNameMap.get(customerId);
                if (name == null) {
                    throw new IOException("UNEXPECTED, customer is not fount");
                }
                // print the transNum to validate that all of customers will be higher than output from step2
                context.write(NullWritable.get(), new Text(name + "," + transNum));
            }
        }
    }


    private final static String STEP1_OUTPUT ="/step1";
    private final static String STEP2_OUTPUT ="/step2";
    private final static String STEP3_OUTPUT ="/final";
    /*
    args[0] ==> Customers
    args[1] ==> Transactions
    args[1] ==> output dir, for example /user/hadoop/query5

     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Project1, Query5 - step1");
        job.setJarByClass(Query5.class);

        // first step is running Query2.
        job.setMapperClass(Query2.CustomersQuery2Mapper.class);
        job.setCombinerClass(Query2.CustomersQuery2Combiner.class);
        job.setReducerClass(Query2.CustomersQuery2Reducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Query2.CustomerQuery2Data.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + STEP1_OUTPUT));

        boolean isSuccess = job.waitForCompletion(true);

        if (isSuccess == false) {
            throw new Exception("1st map reduced job has failed to complete.");
        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Project1, Query5 - step2");
        job2.setJarByClass(Query5.class);

        job2.setMapperClass(Step2Query5Mapper.class);
        job2.setCombinerClass(Step2Query5Combiner.class);
        job2.setReducerClass(Step2Query5Reducer.class);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Step2Query5Data.class);
        FileInputFormat.addInputPath(job2, new Path(args[2] + STEP1_OUTPUT));
        FileOutputFormat.setOutputPath(job2, new Path(args[2] + STEP2_OUTPUT));
        isSuccess = job2.waitForCompletion(true);
        if (isSuccess == false) {
            throw new Exception("2nd map reduced job has failed to complete.");
        }



        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf2, "Project1, Query5 - step3");
        job3.setJarByClass(Query5.class);
        job3.getConfiguration().set(CUSTOMER_PATH,args[0]);
        job3.getConfiguration().set(STEP2_PATH, args[2] + STEP2_OUTPUT + "/part-r-00000");

        job3.setMapperClass(Step3Query5Mapper.class);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[2] + STEP1_OUTPUT));
        FileOutputFormat.setOutputPath(job3, new Path(args[2] + STEP3_OUTPUT));
        isSuccess = job3.waitForCompletion(true);
        if (isSuccess == false) {
            throw new Exception("3rd map reduced task has failed to complete.");
        }

       // System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.exit(0);
    }

  }
