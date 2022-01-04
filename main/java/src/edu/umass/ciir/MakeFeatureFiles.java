package edu.umass.ciir;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MakeFeatureFiles {
    int lineCount = 0;
    String delimiter = ",";
    String doubleQuote = "\"";

    String toFeature(String type, String line) {
        String outputLine = null;
        try {
            Reader in = new FileReader("path/to/file.csv");
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                String columnOne = record.get(0);
                String columnTwo = record.get(1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outputLine;
    }

    void featureizeWithStreams(String collection) {
//        doOneType(collection, "counts_ordered_gap");
//        doOneType(collection, "counts_unordered_gap");
//        doOneType(collection, "counts_unordered_inwindow");
//        doOneType(collection, "count_indoc");

        int numPartitions = 4;

        ExecutorService exec = Executors.newFixedThreadPool(numPartitions);

        // CAREFUL -- all 4 of these will consume more than 850GB disk space.
        // I was able to run the first 2. Suggest running the next 2 individually.
        List<String> fileName = Arrays.asList("counts_ordered_gap","counts_unordered_gap","counts_unordered_inwindow","count_indoc");
        for (String f : fileName) {
            /* ...execute the task to run concurrently as a runnable: */
            exec.execute(new Runnable() {
                public void run() {
                    /* do the work to be done in its own thread */
                    System.out.println("Running in: " + Thread.currentThread());
                    doOneType(collection, f);
                }
            });
        }
        /* Tell the executor that after these steps above, we will be done: */
        exec.shutdown();
        try {
            /* The tasks are now running concurrently. We wait until all work is done,
             * with a timeout of 6 hours: */
            boolean b = exec.awaitTermination(36, TimeUnit.HOURS);
            /* If the execution timed out, false is returned: */
            System.out.println("All done: " + b);
        } catch (InterruptedException e) { e.printStackTrace(); }
    }

    void doOneType(String collection, String type) {
        System.out.println("Doing " + type);
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(Files.newBufferedWriter(
                    Paths.get(collection + "." + type + ".features")));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Reader in = new FileReader(collection);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                String first_term = record.get(0);
                String second_term = record.get(1);
                int index = Integer.parseInt(record.get(2));
                int count = Integer.parseInt(record.get(3));
                Double answer = safe_log(count)
                     + safe_log(totalCollectionFrequency)
                     - safe_log(select count from collection_frequencies where term = first_term)
                     - safe_log(select count from collection_frequencies where term = second_term);
                String outputLine = doubleQuote + first_term + doubleQuote + delimiter + doubleQuote + second_term + doubleQuote
                        + delimiter + index + delimiter + answer;
                pw.println(outputLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        pw.close();
    }
}
