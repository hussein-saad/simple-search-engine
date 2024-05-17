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
            int totalWords = tokenizer.countTokens();
            docId.set(currentDocId + ":" + totalWords);

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().replaceAll("[^a-zA-Z\\p{InArabic}]", "").toLowerCase();
                if (token.isEmpty()) {
                    continue;
                }
                word.set(token);
                context.write(word, docId);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        private final static Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, String> fileWordCount = new HashMap<>();
            for (Text value : values) {
                String[] parts = value.toString().split(":");
                String filename = parts[0];
                String totalWords = parts[1];
                int dotIndex = filename.lastIndexOf('.');
                if (dotIndex != -1) {
                    filename = filename.substring(0, dotIndex);
                }

                fileWordCount.put(filename, fileWordCount.getOrDefault(filename, "0") + 1 + ":" + totalWords);
            }

            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<String, String> entry : fileWordCount.entrySet()) {
                stringBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
            }
            if (stringBuilder.length() > 0) {
                stringBuilder.setLength(stringBuilder.length() - 1);
            }
            result.set(stringBuilder.toString());
            context.write(key, result); // Emit (word, file1:freq1:totalWords1;file2:freq2:totalWords2;...;fileN:freqN:totalWordsN)
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(Main.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
