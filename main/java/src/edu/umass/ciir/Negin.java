package edu.umass.ciir;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.abs;

public class Negin {
    enum StatTypes { counts_unordered_gap, counts_ordered_gap, counts_unordered_inwindow, count_indoc,
    document_frequencies, collection_frequencies, total_collection_term_instances, total_documents}
    int max_half_window = 5;
    Derby d;
    int lineCount = 0;

    void featurize(String collection) throws FileNotFoundException {

        this.d = new Derby();
        d.connect();
        d.createTable();

        int totalDocumentFrequency = 0;
        int totalCollectionFrequency = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(collection))) {
            String line;
            int lineCount = 0;
            while ((line = br.readLine()) != null) {
                if (++lineCount % 10 == 0) {
                    System.out.println(lineCount);
                }
                Set<String> seenTerms = new HashSet<>();
                String[] parts = line.split("\t");
                String[] terms = parts[1].split("[;\\-{} .,/()#¦|'\"]");
                int numTerms = terms.length;

                totalDocumentFrequency += 1;

                for (int i = 0; i < numTerms; ++i) {
                    String curr_term = terms[i];
                    if (curr_term.length() == 0) {
                        continue;
                    }

                    d.bumpTermStat(curr_term, StatTypes.collection_frequencies);

                    ++totalCollectionFrequency;

                    if (!seenTerms.contains(curr_term)) {
                        d.bumpTermStat(curr_term, StatTypes.document_frequencies);
                        seenTerms.add(curr_term);
                    }

                    for (int j = Integer.max(0, i-max_half_window); j < Integer.min(numTerms, i+max_half_window+1); ++j) {
                        String neighbor_term = terms[j];
                        int delta = i - j;
                        int abs_delta = abs(delta);

                        d.bumpPairStat(curr_term, neighbor_term, abs_delta, StatTypes.counts_unordered_gap);
                        if (delta >= 0) {
                            d.bumpPairStat(curr_term, neighbor_term, delta, StatTypes.counts_ordered_gap);
                        } else {
                            d.bumpPairStat(curr_term, neighbor_term, max_half_window + delta, StatTypes.counts_ordered_gap);
                        }
                        for (int k = abs_delta; k < max_half_window+1; ++k) {
                            d.bumpPairStat(curr_term, neighbor_term, k, StatTypes.counts_unordered_inwindow);
                        }
                    }
                }
                for (String term1 : seenTerms) {
                    for (String term2 : seenTerms) {
                        d.bumpPairStat(term1, term2, -1, StatTypes.count_indoc);
                    }
                }
            }

            d.commit();

            d.setGlobalStat(StatTypes.total_collection_term_instances, totalCollectionFrequency);
            d.setGlobalStat(StatTypes.total_documents, totalDocumentFrequency);

            double feature_indoc;

            double[] features_ordered_gap = new double[max_half_window+1];
            double[] features_unordered_gap = new double[max_half_window+1];
            double[] features_unordered_inwindow = new double[max_half_window+1];

            for (Stats stats = d.getFirstPairStat(); stats != null; stats = d.getNextPairStat() ) {
                Integer count = stats.count_indoc;
                Double answer = safe_log(count) + safe_log(totalDocumentFrequency)
                        - safe_log(d.getDocumentFrequency(stats.first)) - safe_log(d.getDocumentFrequency(stats.second));
                feature_indoc = answer;

                Integer[] v = stats.counts_unordered_gap;
                for (int tempIndex = 0; tempIndex < v.length; ++tempIndex) {
                    answer = safe_log(v[tempIndex]) + safe_log(totalCollectionFrequency)
                            - safe_log(d.getCollectionFrequency(stats.first)) - safe_log(d.getCollectionFrequency(stats.second));
                    features_unordered_gap[tempIndex] = answer;
                }

                v = stats.counts_ordered_gap;
                for (int tempIndex = 0; tempIndex < v.length; ++tempIndex) {
                    answer = safe_log(v[tempIndex]) + safe_log(totalCollectionFrequency)
                            - safe_log(d.getCollectionFrequency(stats.first)) - safe_log(d.getCollectionFrequency(stats.second));
                    features_ordered_gap[tempIndex] = answer;
                }

                v = stats.counts_unordered_inwindow;
                for (int tempIndex = 0; tempIndex < v.length; ++tempIndex) {
                    answer = safe_log(v[tempIndex]) + safe_log(totalCollectionFrequency)
                            - safe_log(d.getCollectionFrequency(stats.first)) - safe_log(d.getCollectionFrequency(stats.second))
                            - safe_log(tempIndex*2+1);
                    features_unordered_inwindow[tempIndex] = answer;
                }

                d.addFeature(stats.first, stats.second, feature_indoc, features_ordered_gap, features_unordered_gap,
                        features_unordered_inwindow);
            }

            d.commit();

            System.out.println("totalCollectionFrequency: "  + totalCollectionFrequency);
            System.out.println("totalDocumentFrequency: "  + totalDocumentFrequency);

            int limit = 1000;
            int counter = 0;
            for (FeatureStats stats = d.getFirstFeatureStat(); stats != null && counter < limit; ++counter, stats = d.getNextFeatureStat()) {
                System.out.println("*********************");
                System.out.println(stats.first + " / " + stats.second);
                printFeatureVector("counts_unordered_gap", stats.counts_unordered_gap);
                printFeatureVector("counts_ordered_gap", stats.counts_ordered_gap);
                printFeatureVector("counts_unordered_inwindow", stats.counts_unordered_inwindow);
                System.out.println(String.format("count_indoc: %13.4f", stats.count_indoc));
            }

            d.closeDatabase();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Double safe_log (Integer x) {
        if (x == 0)
            return 0.0;
        return Math.log(x);
    }

    class TermStat {
        String term;
        String type;
        TermStat(String term, String type) {
            this.term = term;
            this.type = type;
        }
        String getType() {
            return type;
        }
        String getKey() {
            return type + term;
        }
        String getTerm() {
            return term;
        }
    }

    class Pair {
        String first_term;
        String second_term;

        public Pair(String first_term, String second_term) {
            this.first_term = first_term;
            this.second_term = second_term;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair pair = (Pair) o;
            return first_term.equals(pair.first_term) && second_term.equals(pair.second_term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first_term, second_term);
        }
    }

    class PairStatKey {
        Pair pair;
        String type;
        int index;

        public PairStatKey(Pair pair, String type, int index) {
            this.pair = pair;
            this.type = type;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PairStatKey that = (PairStatKey) o;
            return index == that.index && pair.equals(that.pair) && type.equals(that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pair, type, index);
        }
    }

    class PairStat {
        String first_term;
        String second_term;
        int index;
        PairStat(String first_term, String second_term) {
            this.first_term = first_term;
            this.second_term = second_term;
            this.index = -1;
        }
        PairStat(String first_term, String second_term, int index) {
            this.first_term = first_term;
            this.second_term = second_term;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PairStat pairStat = (PairStat) o;
            return index == pairStat.index && first_term.equals(pairStat.first_term) && second_term.equals(pairStat.second_term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first_term, second_term, index);
        }
    }

    List<TermStat> toTermStats(String line, StatTypes statType) {
        Set<String> seenTerms = new HashSet<>();
        List<TermStat> termStats = new ArrayList<>();
        String[] parts = line.split("\t");
        String[] terms = parts[1].split("[;\\-{} .,/()#¦|'\"]");
        int numTerms = terms.length;
        for (int i = 0; i < numTerms; ++i) {
            String curr_term = terms[i];
            if (curr_term.length() == 0) {
                continue;
            }
            if (statType == StatTypes.collection_frequencies) {
                termStats.add(new TermStat(curr_term, "collection_frequencies"));
            } else if (statType == StatTypes.document_frequencies) {
                if (!seenTerms.contains(curr_term)) {
                    termStats.add(new TermStat(curr_term, "document_frequencies"));
                    seenTerms.add(curr_term);
                }
            }
        }
        return termStats;
    }

    List<PairStat> toPairStats(String line, StatTypes statType) {
        if (++lineCount %10000 == 0) {
            System.out.println("At " + lineCount);
        }
        //line = line.replace(" ##", "");
        Set<String> seenTerms = new HashSet<>();
        List<PairStat> pairStats = new ArrayList<>();
        String[] parts = line.split("\t");
        String[] terms = parts[1].split("[!;\\-{} .,/()¦|'\"]+");
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

                if (statType == StatTypes.counts_unordered_gap) {
                    pairStats.add(new PairStat(curr_term, neighbor_term, abs_delta));
                }
                if (statType == StatTypes.counts_ordered_gap) {
                    if (delta >= 0) {
                        if (delta != 0)
                            pairStats.add(new PairStat(curr_term, neighbor_term, delta));
                    } else {
                        if (max_half_window + delta != 0)
                            pairStats.add(new PairStat(curr_term, neighbor_term, max_half_window + delta));
                    }
                }
                if (statType == StatTypes.counts_unordered_inwindow) {
                    for (int k = abs_delta; k < max_half_window + 1; ++k) {
                        if (k != 0)
                            pairStats.add(new PairStat(curr_term, neighbor_term, k));
                    }
                }
            }
        }

        if (statType == StatTypes.count_indoc) {
            for (String term1 : seenTerms) {
                for (String term2 : seenTerms) {
                    pairStats.add(new PairStat(term1, term2));
                }
            }
        }

        return pairStats;
    }

    String copyIt(String s) { return s; }

    void featureizeWithStreams(String collection) {
        this.d = new Derby();
        d.connect();
        d.createTable();

/*
        try (Stream<String> stream = Files.lines(Paths.get(collection)).parallel()) {

            stream.map(line -> toTermStats(line, StatTypes.collection_frequencies)).flatMap(termStats -> termStats.stream())
                    .collect(Collectors.groupingBy(TermStat::getTerm, Collectors.counting())).entrySet().stream().forEach(entry -> {
                        String term = entry.getKey();
                        Long count = entry.getValue();
                        d.addFrequencyStat(term, count, StatTypes.collection_frequencies );
                    });
            d.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (Stream<String> stream = Files.lines(Paths.get(collection)).parallel()) {
            stream.map(line -> toTermStats(line, StatTypes.document_frequencies)).flatMap(termStats -> termStats.stream())
                    .collect(Collectors.groupingBy(TermStat::getTerm, Collectors.counting())).entrySet().stream().forEach(entry -> {
                        String term = entry.getKey();
                        Long count = entry.getValue();
                        d.addFrequencyStat(term, count, StatTypes.document_frequencies );
                    });
            d.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }

*/
        try (Stream<String> strm = Files.lines(Paths.get(collection)).parallel()) {

            strm.map(line -> toPairStats(line, StatTypes.counts_ordered_gap)).flatMap(List::stream)
                    .forEach(pairStat -> {
                        d.addPairStat(pairStat);
                    });
            d.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    void printFeatureVector(String header, double[] ea) {
        System.out.println(header + ":");
        System.out.print("[");
        for (int tempx = 0; tempx < ea.length; ++tempx) {
            System.out.print(ea[tempx] + " ");
        }
        System.out.println("]");
    }

    public static void main (String[] args) {
       MakeCountFiles n = new MakeCountFiles();
       try {
           n.featureizeWithStreams("/u02/negin/shared/collectione-tokenized.tsv");
//           n.featureizeWithStreams("/u02/negin/shared/small.tsv");
       } catch (Exception e) {
           e.printStackTrace();
       }
    }
}
