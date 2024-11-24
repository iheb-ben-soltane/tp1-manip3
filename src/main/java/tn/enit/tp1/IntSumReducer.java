package tn.enit.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class IntSumReducer extends Reducer<Text, IntPairWritable, Text, IntPairWritable> {
    private IntPairWritable result = new IntPairWritable();

    public void reduce(Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
        int totalHours = 0;
        int count = 0;

        // Debug input values
        System.out.println("Reducer Key: " + key);

        for (IntPairWritable val : values) {
            System.out.println("Value Received: Hours="+ (totalHours+ val.getFirst()) + ", Count=" + val.getSecond());
            totalHours = totalHours + val.getFirst();
            count += val.getSecond();
        }

        System.out.println("Total Hours: " + totalHours + ", Count: " + count);

        if (count > 0) {
            float avgHours = (float) totalHours / count;
            System.out.println("Average Hours: " + avgHours);

            // Set the result
            result.setFirst(totalHours);
            result.setSecond(count);
            result.setResult(avgHours);
            context.write(key, result);
        }
    }
}