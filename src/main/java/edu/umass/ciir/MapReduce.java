package edu.umass.ciir;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.Math.abs;

public class MapReduce {
    private Configuration conf;
    private Job job;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Mapper.Context context
        ) throws IOException, InterruptedException {
            String type = context.getJobName();
            String delimiter = ",";
            String line = value.toString();
            int max_half_window = 5;
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
                            outputLine = curr_term + delimiter + neighbor_term
                                    + delimiter + abs_delta;
                            word.set(outputLine);
                            context.write(word, one);
                        }
                    }

                    if (type.equals("counts_ordered_gap")) {
                        if (delta >= 0) {
                            if (delta != 0) {
                                outputLine = curr_term + delimiter + neighbor_term + delimiter + delta;
                                pairStats.add(outputLine);
                            }
                        } else {
                            outputLine = curr_term + delimiter + neighbor_term + delimiter + (max_half_window + abs_delta);
                            pairStats.add(outputLine);
                        }
                    }
                    if (type.equals("counts_unordered_inwindow")) {
                        for (int k = abs_delta; k < max_half_window + 1; ++k) {
                            if (k != 0) {
                                outputLine = curr_term + delimiter + neighbor_term + delimiter + k;
                                pairStats.add(outputLine);
                            }
                        }
                    }
                }
            }

            if (type.equals("count_indoc")) {
                for (String term1 : seenTerms) {
                    for (String term2 : seenTerms) {
                        String outputLine = term1 + delimiter + term2;
                        pairStats.add(outputLine);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

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

    public MapReduce(String type, String inputFile, String outputFile) throws Exception {
        conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        job = Job.getInstance(conf, "count_indoc");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
    }

    public boolean runJob() throws IOException, InterruptedException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }
}
