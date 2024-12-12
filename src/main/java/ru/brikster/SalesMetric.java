package ru.brikster;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Slf4j
@ToString
@Getter
public class SalesMetric implements Writable {

    private final Text id;
    private final Text category;
    private final DoubleWritable value;
    private final IntWritable quantity;

    public SalesMetric() {
        this.id = new Text();
        this.category = new Text();
        this.value = new DoubleWritable();
        this.quantity = new IntWritable();
    }

    public void set(String id, String category, double value, int quantity) {
        this.id.set(id);
        this.category.set(category);
        this.value.set(value);
        this.quantity.set(quantity);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        category.write(out);
        value.write(out);
        quantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        category.readFields(in);
        value.readFields(in);
        quantity.readFields(in);
    }

}
