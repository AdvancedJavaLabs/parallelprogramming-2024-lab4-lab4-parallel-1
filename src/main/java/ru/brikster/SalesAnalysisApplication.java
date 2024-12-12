package ru.brikster;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

@Slf4j
public class SalesAnalysisApplication {

    private static final String HDFS_URI = "hdfs://localhost:9000";
    private static final String INPUT_DIR = "/input";
    private static final String OUTPUT_DIR = "/result";

    private static final boolean USE_SALTED_PARTITIONER = true;
    private static final int REDUCERS_COUNT = 1;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: SalesAnalysis <input_dir> <output_dir> <metric_type>");
            System.exit(1);
        }

        Configuration conf = createConfiguration(args[2]);
//        conf.set("mapreduce.input.fileinputformat.split.maxsize", "30000000");
        FileSystem fs = FileSystem.get(conf);

        Path localInputDir = new Path(args[0]);
        Path hdfsInputDir = new Path(INPUT_DIR);
        Path hdfsOutputDir = new Path(OUTPUT_DIR);
        Path localOutputDir = new Path(args[1]);

        setupDirectories(fs, hdfsInputDir, hdfsOutputDir, localOutputDir);

        try (Stream<java.nio.file.Path> files = Files.list(Paths.get(localInputDir.toString()))) {
            files.forEach(file -> {
                try {
                    Path hdfsFilePath = new Path(hdfsInputDir, file.getFileName().toString());
                    fs.copyFromLocalFile(false, true, new Path(file.toString()), hdfsFilePath);
                } catch (IOException e) {
                    log.error("Error copying file to HDFS: {}", file, e);
                }
            });
        }

        Job job = configureJob(conf, args[2], hdfsInputDir, hdfsOutputDir);
        long startMs = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long totalMs = System.currentTimeMillis() - startMs;

        if (success) {
            fs.copyToLocalFile(false, hdfsOutputDir, localOutputDir, true);
            Files.writeString(Paths.get("metrics.txt"), "ms: " + totalMs + ", salted: " + USE_SALTED_PARTITIONER + ", reducers: " + REDUCERS_COUNT +"\n",
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE);
        } else {
            System.err.println("Job failed!");
            System.exit(1);
        }
    }

    private static Configuration createConfiguration(String metricType) {
        Configuration conf = new Configuration();
        conf.set("metric.type", metricType.toUpperCase());
        conf.set("fs.defaultFS", HDFS_URI);
        return conf;
    }

    private static void setupDirectories(FileSystem fs, Path inputDir, Path outputDir, Path localOutputDir) throws IOException {
        if (!fs.exists(inputDir)) {
            fs.mkdirs(inputDir);
        }
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        var localOutputDirPath = Paths.get(localOutputDir.toString());
        if (Files.exists(localOutputDirPath)) {
            try (var files = Files.walk(localOutputDirPath)) {
                files.forEach(path -> {
                    try {
                        if (!Files.isDirectory(path)) {
                            Files.delete(path);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            Files.delete(localOutputDirPath);
        }
    }

    private static Job configureJob(Configuration conf, String metricType, Path inputDir, Path outputDir)
            throws IOException {
        Job job = Job.getInstance(conf, "sales analysis - " + metricType);
        job.setJarByClass(SalesAnalysisApplication.class);

        job.setMapperClass(SalesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesMetric.class);

        job.setReducerClass(SalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (USE_SALTED_PARTITIONER) {
            job.setPartitionerClass(SaltedPartitioner.class);
        }
        job.setNumReduceTasks(REDUCERS_COUNT);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        return job;
    }
}