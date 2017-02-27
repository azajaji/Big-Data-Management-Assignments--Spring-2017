package edu.wpi.ds503.q3;

import edu.wpi.ds503.q1.DataGenerator;

import java.io.PrintWriter;


public class KMeansSeedsAndPointsGenerator extends DataGenerator {
    static final String POINTS_FILE_NAME = "kmeans_dataset";
    static final String KSEED_FILE_NAME = "kmeans_seed";

    static final int MIN = 1;
    static final int MAX = 10000;

    public KMeansSeedsAndPointsGenerator(int numOfRecords) {
        super(numOfRecords);
    }

    public void generate_nb(int nb, String fname ) {
        try{
            int id = 0;
            PrintWriter writer = new PrintWriter(fname, "UTF-8");
            while (id <nb) {
                StringBuilder sb = new StringBuilder();
                sb.append(randomInt(MIN,MAX)).append(" ")
                        .append(randomInt(MIN,MAX));
                writer.println(sb.toString());
                id++;
            }
            writer.close();
        } catch (Exception e) {
            // do something
        }
    }

    public void generate_seed(int k, String fname ) {
        try{
            int id = 0;
            PrintWriter writer = new PrintWriter(fname, "UTF-8");
            while (id <k) {

                StringBuilder sb = new StringBuilder();
                sb.append((int )randomInt(MIN,MAX) ).append(" ")
                        .append((int )randomInt(MIN,MAX) );
                writer.println(sb.toString());
                id++;
            }
            writer.close();
        } catch (Exception e) {
            // do something
        }
    }
    public static void main(String[] args) throws Exception {
        Integer nb_points = Integer.parseInt(args[0]);
        Integer nb_kseeds = Integer.parseInt(args[1]);

        KMeansSeedsAndPointsGenerator instance= new KMeansSeedsAndPointsGenerator(nb_points);
        instance.generate_nb(nb_points,POINTS_FILE_NAME);
        instance.generate_seed(nb_kseeds,KSEED_FILE_NAME);
    }

    @Override
   public void generate() {
        // TODO Auto-generated method stub

    }
}
