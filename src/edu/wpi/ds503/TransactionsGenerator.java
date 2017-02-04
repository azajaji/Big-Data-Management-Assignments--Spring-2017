package edu.wpi.ds503;

import java.io.PrintWriter;

/**
 * Created by yousef fadila on 04/02/2017.
 */
public class TransactionsGenerator extends DataGenerator {
    static final String FILE_NAME = "Transactions";

    public TransactionsGenerator(int numOfRecords) {
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
                .append(rnd.nextInt(50000)+1).append(",") // CustID
                .append(rnd.nextFloat()*990+10).append(",") // TransTotal
                .append(rnd.nextInt(10)+1).append(",") // TransNumItems
                .append(randomString(rnd.nextInt(31)+20)); // TransDesc:
                writer.println(sb.toString());
                id++;
            }
            writer.close();
        } catch (Exception e) {
            // do something
        }
    }
}
