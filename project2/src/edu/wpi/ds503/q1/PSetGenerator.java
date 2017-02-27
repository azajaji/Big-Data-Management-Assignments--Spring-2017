package edu.wpi.ds503.q1;

import java.io.PrintWriter;

/**
 * Created by yousef fadila on 24/02/2017.
 */
public class PSetGenerator extends DataGenerator {
    static final String FILE_NAME = "p_dataset";
    static final int MIN = 1;
    static final int MAX = 10000;

    public PSetGenerator(int numOfRecords) {
        super(numOfRecords);
    }

    @Override
    public void generate() {
        try{
            int id = 1;
            PrintWriter writer = new PrintWriter(FILE_NAME, "UTF-8");
            while (id <=numOfRecords) {
                StringBuilder sb = new StringBuilder();
                sb.append(randomInt(MIN,MAX)).append(",")
                        .append(randomInt(MIN,MAX));
                writer.println(sb.toString());
                id++;
            }
            writer.close();
        } catch (Exception e) {
            // do something
        }
    }
}
