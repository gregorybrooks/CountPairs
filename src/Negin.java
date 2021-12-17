import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static java.lang.Math.abs;

public class Negin {
    Map<Pair, Stats> master = new HashMap<>();
    enum StatTypes { counts_unordered_gap, counts_ordered_gap, counts_unordered_inwindow, count_indoc}
    int max_half_window;

    class Stats {
        Integer count_indoc;
        Integer[] counts_unordered_gap;
        Integer[] counts_ordered_gap;
        Integer[] counts_unordered_inwindow;
        Stats() {
            this.count_indoc = 0;

            int arraySize = max_half_window * 2 + 1;
            Integer[] largeArray = new Integer[arraySize];
            for (int tempx = 0; tempx < arraySize; ++tempx) {
                largeArray[tempx] = 0;
            }

            arraySize = max_half_window + 1;
            Integer[] smallArray = new Integer[arraySize];
            for (int tempx = 0; tempx < arraySize; ++tempx) {
                smallArray[tempx] = 0;
            }

            this.counts_ordered_gap = largeArray;
            this.counts_unordered_gap = smallArray;
            this.counts_unordered_inwindow = smallArray;
        }
    }

    private class Pair {
        String first;
        String second;
        Pair(String first, String second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "first='" + first + '\'' +
                    ", second='" + second + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair pair = (Pair) o;
            return Objects.equals(first, pair.first) && Objects.equals(second, pair.second);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, second);
        }
    }

    void bumpPairCounter(String curr_term, String neighbor_term, Map<Pair,Integer> m) {
        Pair pair = new Pair(curr_term, neighbor_term);
        if (master.containsKey(pair)) {
            Stats stats = master.get(pair);
            stats.count_indoc += 1;
        } else {
            Stats stats = new Stats();
            stats.count_indoc = 1;
            master.put(pair, stats);
        }
    }

    void bumpCounter(Map<String,Integer> m, String term) {
        if (m.containsKey(term)) {
            int current_count = m.get(term);
            m.put(term, ++current_count);
        } else {
            m.put(term, 1);
        }
    }

    void bumpArrayCounter(String curr_term, String neighbor_term, int index, StatTypes statName) {
        Pair pair = new Pair(curr_term, neighbor_term);
        Stats stats;
        if (master.containsKey(pair)) {
            stats = master.get(pair);
        } else {
            stats = new Stats();
        }
        switch (statName) {
            case counts_unordered_gap:
                stats.counts_unordered_gap[index] += 1;
                break;
            case counts_ordered_gap:
                stats.counts_ordered_gap[index] += 1;
                break;
            case counts_unordered_inwindow:
                stats.counts_unordered_inwindow[index] += 1;
                break;
            case count_indoc:
                stats.count_indoc += 1;
                break;
        }
        master.put(pair, stats);
    }


    void featurize(String collection, int max_half_window) throws FileNotFoundException {

        this.max_half_window = max_half_window;

        Map<String, Integer> document_frequencies = new HashMap<>();
        Map<String,Integer> collection_frequencies = new HashMap<>();
        int totalDocumentFrequency = 0;
        int totalCollectionFrequency = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(collection))) {
            String line;
            int lineCount = 0;
            while ((line = br.readLine()) != null) {
                if (++lineCount % 1000 == 0) {
                    System.out.println(lineCount);
                }
                Set<String> seenTerms = new HashSet<>();
                String[] parts = line.split("\t");
                String[] terms = parts[1].split(" ");
                int numTerms = terms.length;

                totalDocumentFrequency += 1;

                for (int i = 0; i < numTerms; ++i) {
                    String curr_term = terms[i];

                    bumpCounter(collection_frequencies, curr_term);

                    ++totalCollectionFrequency;

                    if (!seenTerms.contains(curr_term)) {
                        bumpCounter(document_frequencies, curr_term);
                        seenTerms.add(curr_term);
                    }

                    for (int j = Integer.max(0, i-max_half_window); j < Integer.min(numTerms, i+max_half_window+1); ++j) {
                        String neighbor_term = terms[j];
                        int delta = i - j;
                        int abs_delta = abs(delta);

                        bumpArrayCounter(curr_term, neighbor_term, abs_delta, StatTypes.counts_unordered_gap);

                        if (delta >= 0) {
                            bumpArrayCounter(curr_term, neighbor_term, delta, StatTypes.counts_ordered_gap);
                        } else {
                            bumpArrayCounter(curr_term, neighbor_term, max_half_window + delta, StatTypes.counts_ordered_gap);
                        }
                        for (int k = abs_delta; k < max_half_window+1; ++k) {
                            bumpArrayCounter(curr_term, neighbor_term, k, StatTypes.counts_unordered_inwindow);
                        }
                    }
                }
                for (String term1 : seenTerms) {
                    for (String term2 : seenTerms) {
                        bumpArrayCounter(term1, term2, -1, StatTypes.count_indoc);
                    }
                }
            }

            // feature_indoc = {k: safe_log(v) + safe_log(document_frequencies['']) - safe_log(document_frequencies[k[0]]) - safe_log(document_frequencies[k[1]]) for k,v in count_indoc.items()}
            Map<Pair, Double> feature_indoc = new HashMap<>();
            for (Map.Entry<Pair,Integer> e : count_indoc.entrySet()) {
                Pair k = e.getKey();
                Integer v = e.getValue();
                Double answer = safe_log(v) + safe_log(totalDocumentFrequency) - safe_log(document_frequencies.get(k.first)) - safe_log(document_frequencies.get(k.second));
                feature_indoc.put(k, answer);
            }

            // features_unordered_gap = {k: [safe_log(count) + safe_log(collection_frequencies['']) - safe_log(collection_frequencies[k[0]]) - safe_log(collection_frequencies[k[1]]) for count in v] for k,v in counts_unordered_gap.items()}
            Map<Pair, List<Double>> features_unordered_gap = new HashMap<>();
            for (Map.Entry<Pair,Integer[]> e : counts_unordered_gap.entrySet()) {
                Pair k = e.getKey();
                Integer[] v = e.getValue();
                List<Double> answers = new ArrayList<>();
                for (int tempIndex = 0; tempIndex < v.length; ++tempIndex) {
                    Double answer = safe_log(v[tempIndex]) + safe_log(totalCollectionFrequency) - safe_log(collection_frequencies.get(k.first)) - safe_log(collection_frequencies.get(k.second));
                    answers.add(answer);
                }
                features_unordered_gap.put(k, answers);
            }
            System.out.println("totalCollectionFrequency: "  + totalCollectionFrequency);
            System.out.println("totalDocumentFrequency: "  + totalDocumentFrequency);

            for (Map.Entry<Pair, Stats> e : master.entrySet()) {
                Stats stats = e.getValue();
                Pair pair = e.getKey();
                System.out.println(pair.first + " / " + pair.second);
                Integer[] unordered = stats.counts_unordered_gap;
                Integer[] ordered = stats.counts_ordered_gap;
                Integer[] inwindow = stats.counts_unordered_inwindow;
                System.out.println(String.format("count_indoc: %d", stats.count_indoc));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Double safe_log (Integer x) {
        if (x == 0)
            return 0.0;
        return Math.log(x);
    }
    void printMap(Map<Pair,Integer[]> m) {
        for (Map.Entry e : m.entrySet()) {
            System.out.println(e.getKey());
            Integer[] ea = (Integer[]) e.getValue();
            System.out.print("[");
            for (int tempx = 0; tempx < ea.length; ++tempx) {
                System.out.print(ea[tempx] + " ");
            }
            System.out.println("]");
        }
    }

    void printSimpleMap(Map<Pair,Integer> m) {
        for (Map.Entry e : m.entrySet()) {
            System.out.print(e.getKey());
            System.out.println(" : " + e.getValue());
        }
    }

    public static void main (String[] args) {
       Negin n = new Negin();
       try {
           n.featurize("/u02/negin/shared/collectione-tokenized.tsv", 5);
       } catch (IOException e) {
           e.printStackTrace();
       }
    }
}
