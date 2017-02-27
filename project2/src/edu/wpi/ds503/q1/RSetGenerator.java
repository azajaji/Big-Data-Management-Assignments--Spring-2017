package edu.wpi.ds503.q1;

import edu.wpi.ds503.q1.DataGenerator;

import java.io.PrintWriter;

/**
 * Created by yousef fadila on 24/02/2017.
 */
public class RSetGenerator extends DataGenerator {
    static final String FILE_NAME = "r_dataset";
    static final int MIN = 1;
    static final int MAX = 10000;
    static final int MAX_WIDTH = 5;
    static final int MAX_HEIGHT = 20;

    public RSetGenerator(int numOfRecords) {
        super(numOfRecords);
    }

    @Override
    public void generate() {
        try{
            int id = 1;
            PrintWriter writer = new PrintWriter(FILE_NAME, "UTF-8");
            while (id <=numOfRecords) {
                StringBuilder sb = new StringBuilder();
                int width = randomInt(MIN, MAX_WIDTH);
                int height = randomInt(MIN, MAX_HEIGHT);
                // choose the top-left so that the whole rectangle is in the boundary of 0,max
                int x1= randomInt(MIN, MAX-width);
                int y1 = randomInt(MIN + height ,MAX);

                sb.append("r").append(id).append(",")
                        .append(x1).append(",")
                        .append(y1).append(",")
                        .append(width).append(",")
                        .append(height);
                writer.println(sb.toString());
                id++;
            }
            writer.close();
        } catch (Exception e) {
            // do something
        }
    }
}
