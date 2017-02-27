package edu.wpi.ds503.q3;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public final class PointWritable implements WritableComparable<PointWritable> {

    private IntWritable x;
    private IntWritable y;

    public PointWritable() {
        super();
        x = new IntWritable();
        y = new IntWritable();
    }

    public PointWritable(int x, int y) {
        super();
        set(new IntWritable(x), new IntWritable(y));
    }

    public PointWritable(IntWritable x, IntWritable y) {
        set(x, y);
    }

    public void set(int x, int y) {
        set(new IntWritable(x), new IntWritable(y));
    }

    private void set(IntWritable x, IntWritable y) {
        this.x = x;
        this.y = y;
    }

    public IntWritable getx() {
        return x;
    }

    public IntWritable gety() {
        return y;
    }

    public final void write(DataOutput out) throws IOException {
        x.write(out);
        y.write(out);
    }

    public final void readFields(DataInput in) throws IOException {

        x.readFields(in);
        y.readFields(in);
    }

    @Override
    public int hashCode() {
        return x.get() * 13467 + y.get();
    }

    @Override
    public int compareTo(PointWritable o) {
        // TODO Auto-generated method stub
        int i = x.compareTo(o.x);
        if (i == 0 )
            return y.compareTo(o.y);
        else
            return i;
    }

    public String toString() {
        return String.valueOf(x) + " " +  String.valueOf(y);
    }

    public void fromString(String in) {
        Scanner sr = new Scanner(in);
        set(new IntWritable(sr.nextInt()), new IntWritable(sr.nextInt()));
    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        if (!(obj instanceof PointWritable))
            return false;

        PointWritable o = (PointWritable) obj;
        if (o.x.get() != x.get())
            return false;
        if (o.y.get() != y.get())
            return false;
        return true;
    }
}

