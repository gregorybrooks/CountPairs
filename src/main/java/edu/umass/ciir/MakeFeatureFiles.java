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
//        List<String> fileName = Arrays.asList("counts_ordered_gap","counts_unordered_gap","counts_unordered_inwindow","count_indoc");
        List<String> fileName = Arrays.asList("counts_ordered_gap");
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

    Double safe_log (Integer x) {
        if (x == 0)
            return 0.0;
        return Math.log(x);
    }

    int getTotalCollectionFrequency() {
        return 574008398;
    }

    int getTotalDocumentFrequency() {
        return 8841823;
    }

    Map<String,Integer> getDocumentFrequencies(String collection) {
        Map<String,Integer> documentFrequencies = new HashMap<>();
        try {
            Reader in = new FileReader(collection + ".document_frequencies.csv");
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                String term = record.get(0);
                int count = Integer.parseInt(record.get(1));
                documentFrequencies.put(term, count);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return documentFrequencies;
    }

    Map<String,Integer> getCollectionFrequencies(String collection) {
        Map<String,Integer> collectionFrequencies = new HashMap<>();
        try {
            Reader in = new FileReader(collection + ".collection_frequencies.csv");
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                String term = record.get(0);
                int count = Integer.parseInt(record.get(1));
                collectionFrequencies.put(term, count);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return collectionFrequencies;
    }

    void doOneType(String collection, String type) {
        System.out.println("Doing " + type);

        String infile = collection + "." + type + ".grouped.csv";

        PrintWriter pw = null;
        try {
            pw = new PrintWriter(Files.newBufferedWriter(
                    Paths.get(collection + "." + type + ".features.csv")));
        } catch (IOException e) {
            e.printStackTrace();
        }

        int totalCollectionFrequency = getTotalCollectionFrequency();
        int totalDocumentFrequency = getTotalDocumentFrequency();
        Map<String,Integer> collectionFrequencies = getCollectionFrequencies(collection);
        Map<String,Integer> documentFrequencies = getDocumentFrequencies(collection);

        try {
            Reader in = new FileReader(infile);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                String first_term = record.get(0);
                String second_term = record.get(1);
                int index = Integer.parseInt(record.get(2));
                int count = Integer.parseInt(record.get(3));
                Double answer = safe_log(count)
                     + safe_log(totalCollectionFrequency)
                     - safe_log(collectionFrequencies.get(first_term))
                     - safe_log(collectionFrequencies.get(second_term));
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