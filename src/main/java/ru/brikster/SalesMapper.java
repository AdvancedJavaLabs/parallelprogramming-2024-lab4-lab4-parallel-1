package ru.brikster;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Slf4j
public class SalesMapper extends Mapper<Object, Text, Text, SalesMetric> {

    private MetricType metricType;

    @Override
    protected void setup(Context context) {
        metricType = MetricType.valueOf(context.getConfiguration()
                .get("metric.type", "REVENUE"));
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("transaction_id")) {
            return;
        }

        String[] fields = value.toString().split(",");
        if (fields.length == 5) {
            String id = fields[0];
            String category = fields[2];
            double price = Double.parseDouble(fields[3]);
            int quantity = Integer.parseInt(fields[4]);
            double revenue = price * quantity;

            Text categoryKey = new Text(category);

            SalesMetric metric = new SalesMetric();
            switch (metricType) {
                case REVENUE:
                    metric.set(id, category, revenue, quantity);
                    break;
                case AVERAGE_PRICE:
                    metric.set(id, category, price, quantity);
                    break;
                case ITEMS_SOLD:
                    metric.set(id, category, quantity, quantity);
                    break;
                case AVERAGE_QUANTITY:
                    metric.set(id, category, quantity, 1);
                    break;
                case TRANSACTIONS:
                    metric.set(id, category, 1, quantity);
                    break;
                default:
                    return;
            }

            context.write(categoryKey, metric);
        }
    }

}
