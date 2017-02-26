package edu.wpi.ds503;

import java.util.Random;

/**
 * Created by yousef fadila on 24/02/2017.
 */
public abstract class DataGenerator {
    static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static Random rnd = new Random();

    protected final int numOfRecords;
    public DataGenerator(int numOfRecords) {
        this.numOfRecords = numOfRecords;
    }

    abstract void generate();


    String randomString( int len ) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(AB.charAt(rnd.nextInt(AB.length())));
        return sb.toString();
    }

    int randomInt( int min, int max ) {
        return rnd.nextInt(max - min + 1) + min;
    }
}