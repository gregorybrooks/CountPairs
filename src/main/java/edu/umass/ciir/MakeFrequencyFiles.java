package edu.umass.ciir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class MakeFrequencyFiles {
    private static final String delimiter = ",";
    private static final String doubleQuote = "\"";

    void gatherFrequencyStats(String collection) {
        AtomicInteger totalCollectionFrequency = new AtomicInteger();
        AtomicInteger totalDocumentFrequency = new AtomicInteger();
        Map<String,Integer> collection_frequencies = new ConcurrentHashMap<>();
        Map<String,Integer> document_frequencies = new ConcurrentHashMap<>();
        try (Stream<String> strm = Files.lines(Paths.get(collection)).parallel() ) {
            strm.forEach(line -> {
                ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
                Set<String> seenTerms = map.newKeySet();

                String[] parts = line.split("\t");
                GalagoTokenizer tokenizer = new GalagoTokenizer();
                String[] terms = tokenizer.parseLine(parts[1]);
                totalDocumentFrequency.incrementAndGet();
                for (String curr_term : terms) {
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
