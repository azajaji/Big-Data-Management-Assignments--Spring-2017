package edu.wpi.ds503.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yousef fadila on 10/02/2017.
 */
public class Query3 {
    public static class Query3Data implements WritableComparable<Query3.Query3Data> {

        // set by both Customer & Transactions to do the join
        private IntWritable customerId = new IntWritable();

        // Set by Customer mapper only
        private Text name = new Text();
        private FloatWritable salary = new FloatWritable();

        // Set by Transactions mapper only
        private FloatWritable transTotal = new FloatWritable();
        private IntWritable transItems = new IntWritable();

        public IntWritable getCustomerId() {
            return customerId;
        }

        public Text getName() {
            return name;
        }

        public FloatWritable getSalary() {
            return salary;
        }

        public FloatWritable getTransTotal() {
            return transTotal;
        }

        public IntWritable getTransItems() {
            return transItems;
        }

        public boolean isTransactionRecord ()
        {
            return name.getLength() == 0;
        }

        public Query3.Query3Data setCustomer(IntWritable customerId, Text name, FloatWritable salary)
        {
            this.customerId = customerId;
            this.name = name;
            this.salary = salary;
            return this;
        }

        public Query3.Query3Data setTransaction(IntWritable customerId, FloatWritable transTotal, IntWritable transItems)
        {
            this.customerId = customerId;
            this.transTotal = transTotal;
            this.transItems = transItems;
            return this;
        }

        @Override
        public int compareTo(Query3.Query3Data o) {
            // we won't use this type as key, but as value. compareTo is not important for this usage.
            return customerId.compareTo(o.customerId);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            customerId.write(dataOutput);
            transTotal.write(dataOutput);
            transItems.write(dataOutput);
            name.write(dataOutput);
            salary.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            customerId.readFields(dataInput);
            transTotal.readFields(dataInput);
            transItems.readFields(dataInput);
            name.readFields(dataInput);
            salary.readFields(dataInput);

        }
    }

    public static class TransactionsQuery3Mapper
            extends Mapper<Object, Text, IntWritable, Query3.Query3Data> {

        private final static int CUST_ID = 1;
        private final static int TRANS_TOTAL = 2;
        private final static int ITEMS_NUM = 3;

        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int customerId=Integer.parseInt(fields[CUST_ID]);
            float transTotal=Float.parseFloat(fields[TRANS_TOTAL]);
            int itemsNum = Integer.parseInt(fields[ITEMS_NUM]);

            IntWritable customerIdIntWritable = new IntWritable(customerId);
            FloatWritable totalFloatWritable = new FloatWritable(transTotal);
            IntWritable itemsNumIntWritable = new IntWritable(itemsNum);
            context.write(customerIdIntWritable, new Query3.Query3Data().setTransaction(customerIdIntWritable, totalFloatWritable,itemsNumIntWritable));
        }
    }

    public static class CustomersQuery3Mapper
            extends Mapper<Object, Text, IntWritable, Query3.Query3Data> {

        private final static int CUST_ID = 0;
        private final static int CUST_NAME = 1;
        private final static int CUST_SALARY = 4;


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int customerId=Integer.parseInt(fields[CUST_ID]);
            String name=fields[CUST_NAME];
            float salary=Float.parseFloat(fields[CUST_SALARY]);

            IntWritable customerIdIntWritable = new IntWritable(customerId);
            Text customerNameWritable = new Text(name);
            FloatWritable salaryWritable = new FloatWritable(salary);
            context.write(customerIdIntWritable, new Query3.Query3Data().setCustomer(customerIdIntWritable, customerNameWritable,salaryWritable));
        }
    }

    public static class Query3Reducer
            extends Reducer<IntWritable,Query3.Query3Data,NullWritable,Text> {

        public void reduce(IntWritable key, Iterable<Query3.Query3Data> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            int minItems = Integer.MAX_VALUE;
            float salary = 0;
            String name = "";

            for (Query3.Query3Data val : values) {
                if (val.isTransactionRecord()) {
                    count++;
                    sum += val.getTransTotal().get();
                    minItems = val.getTransItems().get() < minItems ? val.getTransItems().get() : minItems;
                } else {
                    // we except each iterable has exactly one customer details record.
                    name = val.getName().toString();
                    salary = val.getSalary().get();
                }
            }

            Text result = new Text(key.get() + "," + name + "," + salary + "," + count + "," + sum + "," + minItems);
            context.write(NullWritable.get(), result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Project1, Query3");
        job.setJarByClass(Query3.class);
        job.setReducerClass(Query3.Query3Reducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Query3.Query3Data.class);

//        StringBuilder inputPaths = new StringBuilder();
//        inputPaths.append(args[0].toString()).append(",")
//                .append(args[1].toString());
//         Configure remaining aspects of the job
//        FileInputFormat.setInputPaths(job, inputPaths.toString());
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Query3.CustomersQuery3Mapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query3.TransactionsQuery3Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
