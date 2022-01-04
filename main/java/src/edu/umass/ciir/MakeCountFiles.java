package edu.umass.ciir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
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
        String[] terms = parts[1].split("[$«~¡¢£¤¥§©ª¬®°±³´µ¶·¹º»¼¿×æðø`&:!;\\-{} .,/()¦|'\"]+");

        /* java's split() leaves an empty first array element if the sentence happens to start with one of the delimiters.
           Remove it so we don't include pairs with the empty string. */
        if (terms[0].length() == 0) {
            terms = Arrays.copyOfRange(terms, 1, terms.length);
        }
        int numTerms = terms.length;
        for (int i = 0; i < numTerms; ++i) {
            String curr_term = terms[i];
            if (curr_term.length() == 0) {
                continue;
            }
            if (!seenTerms.contains(curr_term)) {
                seenTerms.add(curr_term);
            }
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
                        if (max_half_window + delta != 0) {
                            outputLine = doubleQuote + curr_term + doubleQuote + delimiter + doubleQuote + neighbor_term + doubleQuote
                                    + delimiter + (max_half_window + delta);
                            pairStats.add(outputLine);
                        }
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
                    Paths.get(collection + "." + type + ".counts")));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (Stream<String> strm = Files.lines(Paths.get(collection)).parallel()) {
            strm.map(line -> toPairStats(type, line)).flatMap(List::stream)
                    .forEach(pw::println);
        } catch (IOException e) {
            e.printStackTrace();
        }

        pw.close();
    }

    void bumpCounter(Map<String,Integer> m, String term) {
        if (m.containsKey(term)) {
            int count = m.get(term);
            m.put(term, count + 1);
        } else {
            m.put(term, 1);
        }
    }

    void gatherFrequencyStats(String collection, String type) {
        System.out.println("Doing " + type);

        AtomicInteger totalCollectionFrequency = new AtomicInteger();
        AtomicInteger totalDocumentFrequency = new AtomicInteger();
        Map<String,Integer> collection_frequencies = new HashMap<>();
        Map<String,Integer> document_frequencies = new HashMap<>();
        try (Stream<String> strm = Files.lines(Paths.get(collection)).parallel()) {
            strm.forEach(line -> {
                Set<String> seenTerms = new HashSet<>();
                String[] parts = line.split("\t");
                String[] terms = parts[1].split("[$«~¡¢£¤¥§©ª¬®°±³´µ¶·¹º»¼¿×æðø`&:!;\\-{} .,/()¦|'\"]+");

        /* java's split() leaves an empty first array element if the sentence happens to start with one of the delimiters.
           Remove it so we don't include pairs with the empty string. */
                if (terms[0].length() == 0) {
                    terms = Arrays.copyOfRange(terms, 1, terms.length);
                }
                int numTerms = terms.length;
                totalDocumentFrequency.incrementAndGet();
                for (int i = 0; i < numTerms; ++i) {
                    String curr_term = terms[i];
                    if (curr_term.length() == 0) {
                        continue;
                    }
                    bumpCounter(collection_frequencies, curr_term);
                    totalCollectionFrequency.incrementAndGet();

                    if (!seenTerms.contains(curr_term)) {
                        bumpCounter(document_frequencies, curr_term);
                        seenTerms.add(curr_term);
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Now write out the selected frequency stats
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(Files.newBufferedWriter(
                    Paths.get(collection + ".collection_frequencies.csv")));
        } catch (IOException e) {
            e.printStackTrace();
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
