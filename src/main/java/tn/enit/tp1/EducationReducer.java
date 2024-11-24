package tn.enit.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EducationReducer extends Reducer<Text, IntPairWritable, Text, Text> {
    private final static Text result = new Text();

    public void reduce(Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
        int totalPopulation = 0;
        int educatedPopulation = 0;

        // Aggregate total and educated populations
        for (IntPairWritable val : values) {
            totalPopulation += val.getFirst();
            educatedPopulation += val.getSecond();
        }

        // Calculate the percentage of educated people
        double percentage = (double) educatedPopulation / totalPopulation * 100;

        // Determine if the population is educated or not
        String educationStatus = percentage > 50.0 ? "Instruite" : "Non-instruite";
        result.set(String.format("Total: %d, Educated: %d, Percentage: %.2f%% -> %s",
                totalPopulation, educatedPopulation, percentage, educationStatus));
        context.write(key, result);
    }
}