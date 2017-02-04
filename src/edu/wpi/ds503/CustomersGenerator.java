package edu.wpi.ds503;

import java.io.PrintWriter;

/**
 * Created by yousef fadila on 04/02/2017.
 */
public class CustomersGenerator extends DataGenerator {
    static final String FILE_NAME = "Customers";

    public CustomersGenerator(int numOfRecords) {
        super(numOfRecords);
    }

    @Override
    public void generate() {
        try{
            int id = 1;
            PrintWriter writer = new PrintWriter(FILE_NAME, "UTF-8");
            while (id <=numOfRecords) {
                StringBuilder sb = new StringBuilder();
                sb.append(id).append(",") // ID
                .append(randomString(rnd.nextInt(11)+10)).append(",") // NAME
                .append(rnd.nextInt(61)+10).append(",") // AGE
                .append(rnd.nextInt(10)+1).append(",") // COUNTRY_CODE
                .append(rnd.nextFloat()*9900+100); // SALARY
                writer.println(sb.toString());
                id++;
            }
            writer.close();
        } catch (Exception e) {
            // do something
        }
    }
}
