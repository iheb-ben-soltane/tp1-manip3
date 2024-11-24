package tn.enit.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntPairWritable> {
    private Text maritalStatus = new Text();
    private final static IntPairWritable result = new IntPairWritable();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");

        if (itr.countTokens() >= 14) {
            itr.nextToken();  // Skip age
            itr.nextToken();  // Skip workclass
            itr.nextToken();  // Skip fnlwgt
            itr.nextToken();  // Skip education
            itr.nextToken();  // Skip education-num
            String marital = itr.nextToken(); // marital-status (index 5)
            itr.nextToken();  // Skip occupation
            itr.nextToken();  // Skip relationship
            itr.nextToken();  // Skip race
            itr.nextToken();  // Skip sex
            itr.nextToken();  // Skip capital-gain
            itr.nextToken();  // Skip capital-loss
            String hoursStr = itr.nextToken(); // hours-per-week (index 12)
            itr.nextToken();  // Skip native-country
            itr.nextToken();  // Skip income

            // Parse hours worked per week
            int hoursWorked = 0;
            try {

                hoursWorked = Integer.parseInt(hoursStr.trim());
            } catch (NumberFormatException e) {
                return;
            }

            // Set marital status as key
            maritalStatus.set(marital);

            // Set value as (sum of hours, count of records) using IntPairWritable
            result.setFirst(hoursWorked);  // Set sum of hours worked
            result.setSecond(1);           // Set count of records as 1
            context.write(maritalStatus, result);
        }
    }
}