package tn.enit.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageHoursByMaritalStatus {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageHoursByMaritalStatus <input path> <output path>");
            System.exit(-1);
        }

        // Check if output directory exists and delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "AverageHoursByMaritalStatus");
        job.setJarByClass(AverageHoursByMaritalStatus.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntPairWritable.class);  // Set output value class to IntPairWritable
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}