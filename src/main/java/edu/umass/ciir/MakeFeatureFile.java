package edu.umass.ciir;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.Math.abs;

public class MakeFeatureFile {
    private final Configuration conf;
    private final String type;
    private String job1OutputFile;
    private String finalOutputFile;
    private final String inputFile;
    private final String outputFile;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
        private static final Text word = new Text();
        private static final String delimiter = ",";
        private static final int max_half_window = 5;

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String type = context.getJobName();  // trick: passing the feature type as the job name
            if (!(type.equals("counts_unordered_gap") || type.equals("counts_ordered_gap") ||
                  type.equals("counts_unordered_inwindow") || type.equals("count_indoc"))) {
                throw new AppException("Invalid feature type: " + type);
            }
            String line = value.toString();
            Set<String> seenTerms = new HashSet<>();
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
                            outputLine = curr_term + delimiter + neighbor_term
                                    + delimiter + abs_delta;
                            word.set(outputLine);
                            context.write(word, one);
                        }
                    } else if (type.equals("counts_ordered_gap")) {
                        if (delta >= 0) {
                            if (delta != 0) {
                                outputLine = curr_term + delimiter + neighbor_term + delimiter + delta;
                                word.set(outputLine);
                                context.write(word, one);
                            }
                        } else {
                            outputLine = curr_term + delimiter + neighbor_term + delimiter + (max_half_window + abs_delta);
                            word.set(outputLine);
                            context.write(word, one);
                        }
                    } else if (type.equals("counts_unordered_inwindow")) {
                        for (int k = abs_delta; k < max_half_window + 1; ++k) {
                            if (k != 0) {
                                outputLine = curr_term + delimiter + neighbor_term + delimiter + k;
                                word.set(outputLine);
                                context.write(word, one);
                            }
                        }
                    }
                }
            }
            if (type.equals("count_indoc")) {
                for (String term1 : seenTerms) {
                    for (String term2 : seenTerms) {
                        String outputLine = term1 + delimiter + term2;
                        word.set(outputLine);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class GrouperMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private static int totalCollectionFrequency;
        private static int totalDocumentFrequency;
        private static final Map<String,Integer> collectionFrequencies;
        private static final Map<String,Integer> documentFrequencies;
        // This has to be hard-coded for now since it is used even before the main() method is executed.
        // Later we could make it get the name from an env variable.
        private static final String inputFile = "/mnt/scratch/hadoop/hadoop-3.3.1/collectione-tokenized.tsv";
        private final Text text = new Text();

        private static final String delimiter = ",";

        public static void getTotalFrequencies(String collection) {
            int ret = 0;
            try {
                String fname = collection + ".total_frequencies.csv";
                Reader in = new FileReader(fname);
                Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
                for (CSVRecord record : records) {
                    String type = record.get(0);
                    int count = Integer.parseInt(record.get(1));
                    if (type.equals("totalCollectionFrequency")) {
                        totalCollectionFrequency = count;
                    } else if (type.equals("totalDocumentFrequency")) {
                        totalDocumentFrequency = count;
                    }
                }
            } catch (IOException e) {
                throw new AppException(e);
            }
        }

        public static Map<String,Integer> getDocumentFrequencies(String collection) {
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
                throw new AppException(e);
            }
            return documentFrequencies;
        }

        public static Map<String,Integer> getCollectionFrequencies(String collection) {
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
                throw new AppException(e);
            }
            return collectionFrequencies;
        }

        /* To calculate the features, we need some corpus statistics. These have been previously calculated
           and exist in files in the current directory. To pass the stats to the GrouperMapper's map() method,
           we read the files in when the class comes to life:
         */
        static {
            getTotalFrequencies(inputFile);
            collectionFrequencies = getCollectionFrequencies(inputFile);
            documentFrequencies = getDocumentFrequencies(inputFile);
        }

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            DoubleWritable result = new DoubleWritable();
            String type = context.getJobName();  // trick: passing the feature type as the job name
            if (!(type.equals("counts_unordered_gap") || type.equals("counts_ordered_gap") ||
                    type.equals("counts_unordered_inwindow") || type.equals("count_indoc"))) {
                throw new AppException("Invalid feature type: " + type);
            }
            String line = value.toString();
            String[] terms = line.split(",");
            if (terms.length < 2) {
                throw new IllegalArgumentException("Less than 2 terms in key: " + key);
            }

            if (type.equals("counts_ordered_gap") || type.equals("counts_unordered_gap")) {
                String first_term = terms[0];
                String second_term = terms[1];
                int index = Integer.parseInt(terms[2]);
                int count = Integer.parseInt(terms[3]);
                double answer = safe_log(count)
                        + safe_log(totalCollectionFrequency)
                        - safe_log(collectionFrequencies.get(first_term))
                        - safe_log(collectionFrequencies.get(second_term));
                String outputLine = first_term + delimiter + second_term
                        + delimiter + index;
                result.set(answer);
                text.set(outputLine);
                context.write(text, result);
            } else if (type.equals("counts_unordered_inwindow")) {
                String first_term = terms[0];
                String second_term = terms[1];
                int index = Integer.parseInt(terms[2]);
                int count = Integer.parseInt(terms[3]);
                double answer = safe_log(count)
                        + safe_log(totalCollectionFrequency)
                        - safe_log(collectionFrequencies.get(first_term))
                        - safe_log(collectionFrequencies.get(second_term))
                        - safe_log(index * 2 + 1);
                String outputLine = first_term + delimiter + second_term
                        + delimiter + index;
                result.set(answer);
                text.set(outputLine);
                context.write(text, result);
            } else if (type.equals("count_indoc")) {
                String first_term = terms[0];
                String second_term = terms[1];
                int count = Integer.parseInt(terms[2]);
                double answer = safe_log(count)
                        + safe_log(totalDocumentFrequency)
                        - safe_log(documentFrequencies.get(first_term))
                        - safe_log(documentFrequencies.get(second_term));
                String outputLine = first_term + delimiter + second_term;
                result.set(answer);
                text.set(outputLine);
                context.write(text, result);
            }
        }

        private Double safe_log (Integer x) {
            if (x == 0)
                return 0.0;
            return Math.log(x);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
            ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class DoubleSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public MakeFeatureFile(String type, String inputFile, String outputFile) {
        this.type = type;
        conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    /**
     * Runs a map-reduce job that maps the lines in the corpus to word-pair statistics,
     * depending on the type of statistic passed as the 'type' argument to the MapReduce constructor.
     * Before running this program, put the corpus file into the home directory of your HDFS file system like this:
     *     hdfs dfs -put collectione-tokenized.tsv
     * @return true if all went well, else false
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public boolean runJob1() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, type);
        job.setJarByClass(MakeFeatureFile.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputFile));
        job1OutputFile = outputFile + ".GROUPED";
        FileOutputFormat.setOutputPath(job, new Path(job1OutputFile));
        finalOutputFile = outputFile;
        return job.waitForCompletion(true);
    }

    /**
     * Runs a map-reduce job that calculates the "feature" statistic for each word-pair statistic produced by the
     * first map-reduce job (see above). The feature that is calculated depends on the 'type' argument that was passed
     * to the MapReduce constructor.
     * The final file appears as the file 'part-r-00000' in the HDFS directory whose name is the outputFile arg passed
     * to the MapReduce constructor. To get the file to your local file system you would do:
     *     hdfs dfs -get collectione-tokenized.tsv.counts_ordered_gap.features.csv/part-r-00000
     * @return true if all went well, else false
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public boolean runJob2() throws IOException, InterruptedException, ClassNotFoundException {
        Job job2 = Job.getInstance(conf, type);
        job2.setJarByClass(MakeFeatureFile.class);
        job2.setMapperClass(GrouperMapper.class);
        job2.setReducerClass(DoubleSumReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, new Path(job1OutputFile));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutputFile));
        return job2.waitForCompletion(true);
    }
}
