package edu.umass.ciir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.lang.Math.abs;

public class MakeCountFiles {
    int lineCount = 0;
    String delimiter = ",";
    String doubleQuote = "\"";

    List<String> toPairStats(String type, String line) {
        if (++lineCount %10000 == 0) {
            System.out.println("At " + lineCount);
        }
        int max_half_window=5;
        Set<String> seenTerms = new HashSet<>();
        List<String> pairStats = new ArrayList<>();
        String[] parts = line.split("\t");
        GalagoTokenizer tokenizer = new GalagoTokenizer();
        String[] terms = tokenizer.parseLine(parts[1]);
        int numTerms = terms.length;
        for (int i = 0; i < numTerms; ++i) {
            String curr_term = terms[i];
            if (curr_term.length() == 0) {
                continue;
            }
            seenTerms.add(curr_term);
            for (int j = Integer.max(0, i - max_half_window); j < Integer.min(numTerms, i + max_half_window + 1); ++j) {
                String neighbor_term = terms[j];
                int delta = i - j;
                int abs_delta = abs(delta);

                String outputLine;

                if (type.equals("counts_unordered_gap")) {
                    if (abs_delta != 0) {
                        outputLine = doubleQuote + curr_term + doubleQuote + delimiter + doubleQuote + neighbor_term + doubleQuote
                                + delimiter + abs_delta;
                        pairStats.add(outputLine);
                    }
                }

                if (type.equals("counts_ordered_gap")) {
                    if (delta >= 0) {
                        if (delta != 0) {
                            outputLine = doubleQuote + curr_term + doubleQuote + delimiter + doubleQuote + neighbor_term + doubleQuote
                                    + delimiter + delta;
                            pairStats.add(outputLine);
                        }
                    } else {
                        outputLine = doubleQuote + curr_term + doubleQuote + delimiter + doubleQuote + neighbor_term + doubleQuote
                                + delimiter + (max_half_window + abs_delta);
                        pairStats.add(outputLine);
                    }
                }
                if (type.equals("counts_unordered_inwindow")) {
                    for (int k = abs_delta; k < max_half_window + 1; ++k) {
                        if (k != 0) {
                            outputLine = doubleQuote + curr_term + doubleQuote + delimiter + doubleQuote + neighbor_term + doubleQuote
                                    + delimiter + k;
                            pairStats.add(outputLine);
                        }
                    }
                }
            }
        }

        if (type.equals("count_indoc")) {
            for (String term1 : seenTerms) {
                for (String term2 : seenTerms) {
                    String outputLine = doubleQuote + term1 + doubleQuote + delimiter + doubleQuote + term2 + doubleQuote;
                    pairStats.add(outputLine);
                }
            }
        }

        return pairStats;
    }

    void featureizeWithStreams(String collection, String featureType) {
        int numPartitions = 4;

        ExecutorService exec = Executors.newFixedThreadPool(numPartitions);

        // CAREFUL -- all 4 of these will consume more than 850GB disk space.
        // I was able to run the first 2. Suggest running the next 2 individually.
//        List<String> fileName = Arrays.asList("counts_ordered_gap","counts_unordered_gap","counts_unordered_inwindow","count_indoc");
        List<String> fileName = Arrays.asList(featureType);
        for (String f : fileName) {
            /* ...execute the task to run concurrently as a runnable: */
            exec.execute(() -> {
                /* do the work to be done in its own thread */
                System.out.println("Running in: " + Thread.currentThread());
                doOneType(collection, f);
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

        String outputFile = collection + "." + type + ".counts";

        PrintWriter pw;
        try {
            pw = new PrintWriter(Files.newBufferedWriter(Paths.get(outputFile)));
        } catch (IOException e) {
            System.out.println("Could not create output file " + outputFile);
            throw new AppException(e);
        }

        try (Stream<String> strm = Files.lines(Paths.get(collection)).parallel()) {
            strm.map(line -> toPairStats(type, line)).flatMap(List::stream)
                    .forEach(pw::println);
        } catch (IOException e) {
            e.printStackTrace();
        }

        pw.close();
    }

    void gatherFrequencyStats(String collection) {
        AtomicInteger totalCollectionFrequency = new AtomicInteger();
        AtomicInteger totalDocumentFrequency = new AtomicInteger();
        Map<String,Integer> collection_frequencies = new ConcurrentHashMap<>();
        Map<String,Integer> document_frequencies = new ConcurrentHashMap<>();
        try (Stream<String> strm = Files.lines(Paths.get(collection)).parallel() ) {
            strm.forEach(line -> {

//                Set<String> seenTerms = new HashSet<>();
                ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
                Set<String> seenTerms = map.newKeySet();

                String[] parts = line.split("\t");
                GalagoTokenizer tokenizer = new GalagoTokenizer();
                String[] terms = tokenizer.parseLine(parts[1]);
                totalDocumentFrequency.incrementAndGet();
                for (String curr_term : terms) {
//                    if (curr_term.equals("fingertips")) {
//                        System.out.println(parts[0]);
//                    }
                    if (curr_term.length() == 0) {
                        continue;
                    }
                    collection_frequencies.merge(curr_term, 1, Integer::sum);
                    totalCollectionFrequency.incrementAndGet();

                    if (!seenTerms.contains(curr_term)) {
                        document_frequencies.merge(curr_term, 1, Integer::sum);
                        seenTerms.add(curr_term);
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Now write out the selected frequency stats
        String outputFile = collection + ".collection_frequencies.csv";

        PrintWriter pw;
        try {
            pw = new PrintWriter(Files.newBufferedWriter(Paths.get(outputFile)));
        } catch (IOException e) {
            System.out.println("Could not create output file " + outputFile);
            throw new AppException(e);
        }

        for (Map.Entry<String,Integer> e : collection_frequencies.entrySet()) {
            String outputLine = doubleQuote + e.getKey() + doubleQuote + delimiter + e.getValue();
            pw.println(outputLine);
        }
        pw.close();

        try {
            pw = new PrintWriter(Files.newBufferedWriter(
                    Paths.get(collection + ".document_frequencies.csv")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Map.Entry<String,Integer> e : document_frequencies.entrySet()) {
            String outputLine = doubleQuote + e.getKey() + doubleQuote + delimiter + e.getValue();
            pw.println(outputLine);
        }
        pw.close();

        try {
            pw = new PrintWriter(Files.newBufferedWriter(
                    Paths.get(collection + ".total_frequencies.csv")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        pw.println("totalCollectionFrequency" + "," + totalCollectionFrequency);
        pw.println("totalDocumentFrequency" + "," + totalDocumentFrequency);
        pw.close();
    }
}
