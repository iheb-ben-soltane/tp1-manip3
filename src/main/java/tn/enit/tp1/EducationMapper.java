package tn.enit.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EducationMapper extends Mapper<Object, Text, Text, IntPairWritable> {
    private final static Text educationStatusKey = new Text("EducationStatus");
    private final static IntPairWritable result = new IntPairWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(","); // Assuming CSV format

        if (fields.length > 4) { // Check if the line has enough fields
            String education = fields[3].trim(); // Assuming `education` is in index 3

            // Check if the education level is considered "educated"
            int isEducated = 0;
            if (isEducatedLevel(education)) {
                isEducated = 1;
            }

            // Write output as (total_population = 1, educated_population = isEducated)
            result.setFirst(1); // Total population
            result.setSecond(isEducated); // Educated population
            context.write(educationStatusKey, result);
        }
    }

    private boolean isEducatedLevel(String education) {
        // Define levels that are considered "educated"
        return education.equals("Bachelors") || education.equals("Masters") || education.equals("Doctorate")
                || education.equals("Prof-school") || education.equals("Some-college");
    }
}