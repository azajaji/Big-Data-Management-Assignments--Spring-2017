package edu.wpi.ds503.q1;

public class DataGeneratorMain {

    public static void main(String[] args) {

        DataGenerator[] dataGenerators = new DataGenerator[]{ new PSetGenerator(15000000), new RSetGenerator(15000000)};

        for (DataGenerator dataGenerator : dataGenerators) {
            dataGenerator.generate();
        }
    }
}
