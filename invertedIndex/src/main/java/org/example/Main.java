package org.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final static Text word = new Text();
        private final static Text docId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String currentDocId = ((FileSplit) context.getInputSplit()).getPath().getName(); // Extracting document ID

            Map<String, Integer> wordCountMap = new HashMap<>();
            int totalWords = 0;


            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().replaceAll("[^a-zA-Z\\p{InArabic}]", "").toLowerCase();
                if (token.isEmpty()) {
                    continue;
                }
                totalWords++;
                wordCountMap.put(token, wordCountMap.getOrDefault(token, 0) + 1);
            }

            for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                word.set(entry.getKey());
                docId.set(currentDocId + ":" + entry.getValue() + ":" + totalWords);
                context.write(word, docId);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        private final static Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> fileWordCount = new HashMap<>();
            Map<String, Integer> fileTotalWords = new HashMap<>();

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                String filename = parts[0];
                int count = Integer.parseInt(parts[1]);
                int totalWords = Integer.parseInt(parts[2]);

                int dotIndex = filename.lastIndexOf('.');
                if (dotIndex != -1) {
                    filename = filename.substring(0, dotIndex);
                }

                fileWordCount.put(filename, fileWordCount.getOrDefault(filename, 0) + count);
                fileTotalWords.put(filename, totalWords);
            }

            StringBuilder stringBuilder = new StringBuilder();
            for (String filename : fileWordCount.keySet()) {
                stringBuilder.append(filename).append(":")
                        .append(fileWordCount.get(filename)).append(":")
                        .append(fileTotalWords.get(filename)).append(";");
            }
            if (stringBuilder.length() > 0) {
                stringBuilder.setLength(stringBuilder.length() - 1); // Remove trailing semicolon
            }
            result.set(stringBuilder.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(Main.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
