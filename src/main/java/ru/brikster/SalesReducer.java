package ru.brikster;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Slf4j
public class SalesReducer extends Reducer<Text, SalesMetric, Text, Text> {

    private MetricType metricType;
    private List<Result> results;

    @Override
    protected void setup(Context context) {
        metricType = MetricType.valueOf(context.getConfiguration().get("metric.type", "REVENUE"));
        results = new ArrayList<>();
    }

    @Override
    public void reduce(Text key, Iterable<SalesMetric> values, Context context) {
        double totalValue = 0;
        double totalCount = 0;
        double totalQuantity = 0;

        for (SalesMetric val : values) {
            totalValue += val.getValue().get();
            totalCount += val.getCount().get();
            totalQuantity += val.getQuantity().get();
        }

        double metric;

        String result = switch (metricType) {
            case REVENUE -> {
                metric = totalValue;
                yield String.format("%.2f\t%.0f", totalValue, totalQuantity);
            }
            case AVERAGE_PRICE -> {
                metric = totalValue / totalCount;
                yield String.format("%.2f (avg price per item)", totalValue / totalCount);
            }
            case ITEMS_SOLD -> {
                metric = totalValue;
                yield String.format("%.0f items", totalValue);
            }
            case AVERAGE_QUANTITY -> {
                metric = totalValue / totalCount;
                yield String.format("%.2f (avg items per transaction)", totalValue / totalCount);
            }
            case TRANSACTIONS -> {
                metric = totalValue;
                yield String.format("%.0f transactions", totalValue);
            }
        };

        results.add(new Result(new Text(key.toString()), metric, result));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        results.sort(Comparator.comparingDouble(Result::value).reversed());

        for (Result result : results) {
            context.write(result.category(), new Text(result.output()));
        }

        this.results.clear();
    }

    private record Result(Text category, double value, String output) {}

}
