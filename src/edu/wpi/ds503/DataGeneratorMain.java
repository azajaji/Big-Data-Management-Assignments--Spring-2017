package edu.wpi.ds503;

public class DataGeneratorMain {

    public static void main(String[] args) {
        // create samples (10 in total)
        // DataGenerator[] dataGenerators = new DataGenerator[]{ new CustomersGenerator(10),new TransactionsGenerator(10) };
        // create real data, 50,000 cusotmers and 5,000,000 transactions
        DataGenerator[] dataGenerators = new DataGenerator[]{ new CustomersGenerator(50000),new TransactionsGenerator(5000000) };

        for (DataGenerator dataGenerator : dataGenerators) {
            dataGenerator.generate();
        }
    }
}
