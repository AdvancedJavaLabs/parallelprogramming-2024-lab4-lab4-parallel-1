package ru.brikster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SaltedPartitioner extends Partitioner<Text, SalesMetric> {

    @Override
    public int getPartition(Text key, SalesMetric value, int numReduceTasks) {
        String saltedKey = (Math.random() * 10) + key.toString();
        return Math.abs(saltedKey.hashCode() % numReduceTasks);
    }

}