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
    String delimiter = ",";
    String doubleQuote = "\"";

    void featureizeWithStreams(String collection) {

        int numPartitions = 4;

        ExecutorService exec = Executors.newFixedThreadPool(numPartitions);

        // CAREFUL -- Running all 4 of these concurrently will consume a lot of disk space.
//        List<String> type = Arrays.asList("counts_ordered_gap","counts_unordered_gap","counts_unordered_inwindow","count_indoc");
        List<String> type = Arrays.asList("counts_ordered_gap");
        for (String f : type) {
            /* ...execute the task to run concurrently as a runnable: */
            exec.execute(() -> {
                System.out.println("Running " + type + " in thread: " + Thread.currentThread());
                doOneType(collection, f);
            });
        }
        /* Tell the executor that after these steps above, we will be done: */
        exec.shutdown();
        try {
            /* The tasks are now running concurrently. We wait until all work is done,
             * with a timeout of 36 hours: */
            boolean b = exec.awaitTermination(36, TimeUnit.HOURS);
            /* If the execution timed out, false is returned: */
            if (b) {
                System.out.println("All tasks are finished");
            } else {
                System.out.println("TIME-OUT OCCURRED! PROCESSING IS INCOMPLETE!");
            }
        } catch (InterruptedException e) { e.printStackTrace(); }
    }

    Double safe_log (Integer x) {
        if (x == 0)
            return 0.0;
        return Math.log(x);
    }

    /*
            totalCollectionFrequency,574008398
        totalDocumentFrequency,8841823

     */
    int getTotalCollectionFrequency(String collection) {
        int ret = 0;
        try {
            String fname = collection + ".total_frequencies.csv";
            Reader in = new FileReader(fname);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                String type = record.get(0);
                int count = Integer.parseInt(record.get(1));
                if (type.equals("totalCollectionFrequency")) {
                    ret = count;
                }
            }
            if (ret == 0) {
                throw new IllegalArgumentException("totalCollectionFrequency not found in " + fname);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    int getTotalDocumentFrequency(String collection) {
        int ret = 0;
        try {
            String fname = collection + ".total_frequencies.csv";
            Reader in = new FileReader(fname);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            for (CSVRecord record : records) {
                String type = record.get(0);
                int count = Integer.parseInt(record.get(1));
                if (type.equals("totalDocumentFrequency")) {
                    ret = count;
                }
            }
            if (ret == 0) {
                throw new IllegalArgumentException("totalDocumentFrequency not found in " + fname);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
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

        String inputFile = collection + "." + type + ".grouped.csv";
        String outputFile = collection + "." + type + ".features.csv";

        PrintWriter pw;
        try {
            pw = new PrintWriter(Files.newBufferedWriter(Paths.get(outputFile)));
        } catch (IOException e) {
            System.out.println("Could not create output file " + outputFile);
            throw new AppException(e);
        }

        int totalCollectionFrequency = getTotalCollectionFrequency(collection);
        int totalDocumentFrequency = getTotalDocumentFrequency(collection);
        Map<String,Integer> collectionFrequencies = getCollectionFrequencies(collection);
        Map<String,Integer> documentFrequencies = getDocumentFrequencies(collection);

        try {
            if (type.equals("counts_ordered_gap") || type.equals("counts_unordered_gap")) {
                Reader in = new FileReader(inputFile);
                Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
                for (CSVRecord record : records) {
                    String first_term = record.get(0);
                    String second_term = record.get(1);
                    int index = Integer.parseInt(record.get(2));
                    int count = Integer.parseInt(record.get(3));
                    double answer = safe_log(count)
                            + safe_log(totalCollectionFrequency)
                            - safe_log(collectionFrequencies.get(first_term))
                            - safe_log(collectionFrequencies.get(second_term));
                    String outputLine = doubleQuote + first_term + doubleQuote + delimiter + doubleQuote + second_term + doubleQuote
                            + delimiter + index + delimiter + answer;
                    pw.println(outputLine);
                }
            } else {
                throw new IllegalArgumentException("Invalid feature type requested: " + type);
            }
        } catch (IOException e) {
            System.out.println("Exception while reading from " + inputFile + " and writing to "
                         + outputFile);
            throw new AppException(e);
        }
        pw.close();
    }
}
