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
    enum StatTypes { counts_unordered_gap, counts_ordered_gap, counts_unordered_inwindow}

    class Stats {
        Integer count_indoc;
        Integer[] counts_unordered_gap;
        Integer[] counts_ordered_gap;
        Integer[] counts_unordered_inwindow;
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

    void bumpArrayCounter(String curr_term, String neighbor_term, int array_size, int index, StatTypes statName) {
        Pair pair = new Pair(curr_term, neighbor_term);
        if (master.containsKey(pair)) {
            Stats stats = master.get(pair);
            if (statName == StatTypes.counts_unordered_gap) {
                stats.counts_unordered_gap[index] += 1;
            } else if (statName == StatTypes.counts_ordered_gap) {
                stats.counts_ordered_gap[index] += 1;
            } else if (statName == StatTypes.counts_unordered_inwindow) {
                stats.counts_unordered_inwindow[index] += 1;
            }
        } else {
            Integer[] y = new Integer[array_size+1];
            for (int tempx = 0; tempx < array_size; ++tempx) {
                y[tempx] = 0;
            }
            Stats stats = new Stats();
            stats.counts_unordered_gap = y;
            stats.counts_ordered_gap = y;
            stats.counts_unordered_inwindow = y;
            y[index] = 1;
            if (statName.equals("counts_unordered_gap")) {
                stats.counts_unordered_gap = y;
            } else if (statName.equals("counts_ordered_gap")) {
                stats.counts_ordered_gap = y;
            } else if (statName.equals("counts_unordered_inwindow")) {
                stats.counts_unordered_inwindow = y;
            }
            master.put(pair, stats);
        }
    }


    void featurize(String collection, int max_half_window) throws FileNotFoundException {

        Map<String, Integer> document_frequencies = new HashMap<>();
        //collection_frequencies = defaultdict(int)
        Map<String,Integer> collection_frequencies = new HashMap<>();
        //count_indoc = defaultdict(int)
        Map<Pair,Integer> count_indoc = new HashMap<>();
        //counts_unordered_gap = defaultdict(lambda: [0 for i in range(max_half_window+1)])
        Map<Pair, Integer[]> counts_unordered_gap = new HashMap<>();
        //counts_ordered_gap = defaultdict(lambda: [0 for i in range(max_half_window*2+1)])
        Map<Pair, Integer[]> counts_ordered_gap = new HashMap<>();
        //counts_unordered_inwindow = defaultdict(lambda: [0 for i in range(max_half_window+1)])
        Map<Pair, Integer[]> counts_unordered_inwindow = new HashMap<>();
        int totalDocumentFrequency = 0;
        int totalCollectionFrequency = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(collection))) {
            String line;
            while ((line = br.readLine()) != null) {
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

                        bumpArrayCounter(curr_term, neighbor_term, max_half_window, abs_delta, "counts_unordered_gap");

                        if (delta >= 0) {
                            bumpArrayCounter(curr_term, neighbor_term, max_half_window*2, delta, counts_ordered_gap);
                        } else {
                            bumpArrayCounter(curr_term, neighbor_term, max_half_window, max_half_window + delta, counts_ordered_gap);
                        }
                        for (int k = abs_delta; k < max_half_window+1; ++k) {
                            bumpArrayCounter(curr_term, neighbor_term, max_half_window, k, counts_unordered_inwindow);
                        }

                    }

                }
                for (String term1 : seenTerms) {
                    for (String term2 : seenTerms) {
                        bumpPairCounter(term1, term2, count_indoc);
                    }
                }
            }
            System.out.println("totalCollectionFrequency: "  + totalCollectionFrequency);
            System.out.println("totalDocumentFrequency: "  + totalDocumentFrequency);
            for (Map.Entry e : counts_unordered_gap.entrySet()) {
                System.out.println(e.getKey());
                Integer[] ea = (Integer[]) e.getValue();
                System.out.println(ea);
            }
            System.out.println(counts_unordered_gap);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {
       Negin n = new Negin();
       try {
           n.featurize("/u02/negin/JavaVersion/smaller.tsv", 3);
       } catch (IOException e) {
           e.printStackTrace();
       }
    }
}
