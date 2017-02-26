package edu.wpi.ds503;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public final class PointWritable implements WritableComparable<PointWritable> {

    private FloatWritable x;
    private FloatWritable y;

    public PointWritable() {
    	super();
    	x = new FloatWritable();
    	y = new FloatWritable();
    	
    }
    public PointWritable(float x, float y) {
        super();
        set(new FloatWritable(x), new FloatWritable(y));
    }

    public PointWritable(FloatWritable x, FloatWritable y) {
        set(x, y);
    }
    
    public void set(float x, float y) {
    	set(new FloatWritable(x), new FloatWritable(y));
    }
    

    private void set(FloatWritable x, FloatWritable y) {
        this.x = x;
        this.y = y;
    }

    public FloatWritable getx() {
        return x;
    }

    public FloatWritable gety() {
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
        return x.hashCode() * 200 + y.hashCode();
    }

    @Override
    public int compareTo(PointWritable o) {
		// TODO Auto-generated method stub
		return (int) (Math.pow( this.x.get()-o.x.get(),2)+ Math.pow( this.y.get()-o.y.get(),2));
	}



    public String toString() {
		return String.valueOf(x) + " " +  String.valueOf(y);
	}


}


//public class PointWritable implements WritableComparable<PointWritable> {
//
//    private FloatWritable x,y;	
//
//    public PointWritable() {
//	this.x = new FloatWritable();
//	this.y = new FloatWritable();		
//    }
//	
//    public void set ( float a, float b)
//    {
//	this.x.set(a);
//	this.y.set(b);	
//    }
//	
//    
//    @Override
//    public void readFields(DataInput in) throws IOException {
//	x.readFields(in);
//	y.readFields(in);
//    }
//
//    @Override
//    public void write(DataOutput out) throws IOException {
//	x.write(out);
//	y.write(out);
//    }
//	
//	
//    public FloatWritable getx() {
//	return x;
//    }
//
//    public FloatWritable gety() {
//	return y;
//    }
//
//
//
//	@Override
//	public int compareTo(PointWritable o) {
//		// TODO Auto-generated method stub
//		return (int) (Math.pow( this.x.get()-o.x.get(),2)+ Math.pow( this.y.get()-o.y.get(),2));
//	}
//
//	@Override
//	public String toString() {
//		return String.valueOf(x) + " " +  String.valueOf(y);
//	}
//
//}