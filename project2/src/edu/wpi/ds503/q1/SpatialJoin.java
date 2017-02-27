package edu.wpi.ds503.q1;

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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yousef fadila on 24/02/2017.
 */
public class SpatialJoin {
    public static final String WINDOW_KEY = "window";

    // each grid is identified by bottom-left point of the cell.
    public static class Grid implements WritableComparable<Grid> {
        static public int GRID_WIDTH = 5;
        static public int GRID_HEIGHT = 20;

        private IntWritable x = new IntWritable();
        private IntWritable y = new IntWritable();

        public Grid setGridByPoint(int x, int y)
        {
            int gridX = x/GRID_WIDTH;
            int gridY = y/GRID_HEIGHT;

            this.x.set(gridX);
            this.y.set(gridY);
            return this;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            x.write(dataOutput);
            y.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            x.readFields(dataInput);
            y.readFields(dataInput);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Grid grid = (Grid) o;

            if (x != null ? !x.equals(grid.x) : grid.x != null) return false;
            return y != null ? y.equals(grid.y) : grid.y == null;
        }

        @Override
        public int hashCode() {
            int result = x != null ? x.hashCode() : 0;
            result = 31 * result + (y != null ? y.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return x.get() + "," + y.get();
        }

        @Override
        public int compareTo(Grid o) {
            return this.toString().compareTo(o.toString());
        }
    }
    public static class SpatialJoinData implements Writable {

        private IntWritable x;
        private IntWritable y;
        private IntWritable width;
        private IntWritable height;
        private Text id;

        public SpatialJoinData() {
            x = new IntWritable();
            y = new IntWritable();
            width = new IntWritable();
            height = new IntWritable();
            id = new Text();
        }

        public SpatialJoinData(SpatialJoinData clone) {
            this.x = new IntWritable(clone.x.get());
            this.y = new IntWritable(clone.y.get());
            this.width = new IntWritable(clone.width.get());
            this.height = new IntWritable(clone.height.get());
            this.id = new Text(clone.id.toString());
        }

        public SpatialJoinData setPoint(int x, int y)
        {
            this.x.set(x);
            this.y.set(y);
            return this;
        }

        public SpatialJoinData setWidth(int width)
        {
            this.width.set(width);
            return this;
        }

        public SpatialJoinData setHeight(int height)
        {
            this.height.set(height);
            return this;
        }

        public SpatialJoinData setId(String rid)
        {
            this.id.set(rid);
            return this;
        }

        public boolean isRectangle()
        {
            return this.width.get() != 0 && this.height.get() != 0 && !this.id.equals("") ;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            x.write(dataOutput);
            y.write(dataOutput);
            width.write(dataOutput);
            height.write(dataOutput);
            id.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            x.readFields(dataInput);
            y.readFields(dataInput);
            width.readFields(dataInput);
            height.readFields(dataInput);
            id.readFields(dataInput);
        }

        public boolean includePoint(int px, int py) {
             if ((px >= this.x.get()) && (px <= this.x.get()+width.get())
                    && (py <= this.y.get()) && (py >= this.y.get()-height.get()) )
                return true;
            return false;
        }


        public boolean overlapRect(SpatialJoinData rect) {

            if (this.x.get() > rect.x.get() + rect.width.get() || rect.x.get() > this.x.get() + this.width.get())
                return false;

            if (this.y.get() < rect.y.get() - rect.height.get() || rect.y.get() < this.y.get() - this.height.get())
                return false;

            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SpatialJoinData that = (SpatialJoinData) o;

            if (x != null ? !x.equals(that.x) : that.x != null) return false;
            if (y != null ? !y.equals(that.y) : that.y != null) return false;
            if (width != null ? !width.equals(that.width) : that.width != null) return false;
            if (height != null ? !height.equals(that.height) : that.height != null) return false;
            return id != null ? id.equals(that.id) : that.id == null;
        }

        @Override
        public String toString() {
            return "{isRect: " + isRectangle() +
                    " id=" + id +
                    ", x=" + x +
                    ", y=" + y +
                    '}';
        }

        @Override
        public int hashCode() {
            int result = x != null ? x.hashCode() : 0;
            result = 31 * result + (y != null ? y.hashCode() : 0);
            result = 31 * result + (width != null ? width.hashCode() : 0);
            result = 31 * result + (height != null ? height.hashCode() : 0);
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }
    }

    public static class RectanglesMapper
            extends Mapper<Object, Text, Grid, SpatialJoinData> {
        SpatialJoinData window = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            String windowString = context.getConfiguration().get(WINDOW_KEY);
            if (windowString != null) {
                String[] fields = windowString.split(",");
                int x1=Integer.parseInt(fields[0]);
                int y1=Integer.parseInt(fields[1]);
                int x2=Integer.parseInt(fields[2]);
                int y2=Integer.parseInt(fields[3]);
                window = new SpatialJoinData().setHeight(y2-y1).setWidth(x2-x1).setPoint(x1,y2);
            }
    }
        //each line is: r1,2802,7731,5,14
        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String id =fields[0];
            int x=Integer.parseInt(fields[1]);
            int y=Integer.parseInt(fields[2]);
            int width=Integer.parseInt(fields[3]);
            int height = Integer.parseInt(fields[4]);

            SpatialJoinData rectangle = new SpatialJoinData().setId(id).setHeight(height).setWidth(width).setPoint(x,y);

            if (window != null && !window.overlapRect(rectangle)) {
                return;
            }

            context.write(new Grid().setGridByPoint(x,y), rectangle);

            if (x / Grid.GRID_WIDTH != (x + width) / Grid.GRID_WIDTH)
                context.write(new Grid().setGridByPoint(x + width, y), rectangle);

            if (y / Grid.GRID_HEIGHT != (y - height) / Grid.GRID_HEIGHT)
                context.write(new Grid().setGridByPoint(x, y - height), rectangle);

            if ((y / Grid.GRID_HEIGHT != (y - height) / Grid.GRID_HEIGHT) && (x / Grid.GRID_WIDTH != (x + width) / Grid.GRID_WIDTH))
                context.write(new Grid().setGridByPoint(x + width, y - height), rectangle);
        }
    }

    public static class PointsMapper
            extends Mapper<Object, Text, Grid, SpatialJoinData> {
        SpatialJoinData window = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            String windowString = context.getConfiguration().get(WINDOW_KEY);
            if (windowString != null) {
                String[] fields = windowString.split(",");
                int x1 = Integer.parseInt(fields[0]);
                int y1 = Integer.parseInt(fields[1]);
                int x2 = Integer.parseInt(fields[2]);
                int y2 = Integer.parseInt(fields[3]);
                window = new SpatialJoinData().setHeight(y2 - y1).setWidth(x2 - x1).setPoint(x1, y2);
            }
        }
        // each line is: 5785,2768
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int x=Integer.parseInt(fields[0]);
            int y=Integer.parseInt(fields[1]);
            SpatialJoinData point = new SpatialJoinData().setPoint(x, y);
            if (window != null && !window.includePoint(x,y)) {
                return;
            }
            context.write(new Grid().setGridByPoint(x, y), point);
        }
    }

    public static class SpatialJoinReducer
            extends Reducer<Grid,SpatialJoinData,NullWritable,Text> {

        public void reduce(Grid key, Iterable<SpatialJoinData> values, Context context) throws IOException, InterruptedException {
            List<SpatialJoinData> rects = new ArrayList<>();
            List<SpatialJoinData> points = new ArrayList<>();

            for (SpatialJoinData val : values) {
                if (val.isRectangle()) {
                    rects.add(new SpatialJoinData(val));
                } else {
                    points.add(new SpatialJoinData(val));
                }
            }

            for (int i=0; i< rects.size(); i++) {
                for (int j=0; j< points.size(); j++){
                    if (rects.get(i).includePoint(points.get(j).x.get(), points.get(j).y.get())) {
                        context.write(NullWritable.get(), new Text("<" + rects.get(i).id.toString() + ",(" + points.get(j).x.get() + "," + points.get(j).y.get() + ")>"));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Project2, Query1");
        job.setJarByClass(SpatialJoin.class);
        job.setReducerClass(SpatialJoinReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Grid.class);
        job.setMapOutputValueClass(SpatialJoinData.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectanglesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        if (args.length > 3) {
            // spatial window is defined. W(x1,y1,x2,y2) - remove W().
            job.getConfiguration().set(WINDOW_KEY,args[3].substring(2, args[3].length()-1));
        }
        job.submit();
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
