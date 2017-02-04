package edu.wpi.ds503;

public class DataGeneratorMain {

    public static void main(String[] args) {
        DataGenerator[] dataGenerators = new DataGenerator[]{ new CustomersGenerator(10) };

        for (DataGenerator dataGenerator : dataGenerators) {
            dataGenerator.generate();
        }
    }
}
