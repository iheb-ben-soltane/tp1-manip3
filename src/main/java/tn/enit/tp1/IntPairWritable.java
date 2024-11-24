package tn.enit.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPairWritable implements Writable {
    private IntWritable first;
    private IntWritable second;
    private FloatWritable result;

    public IntPairWritable() {
        this.first = new IntWritable();
        this.second = new IntWritable();
        this.result = new FloatWritable();
    }

    public IntPairWritable(int first, int second,float result) {
        this.first = new IntWritable(first);
        this.second = new IntWritable(second);
        this.result = new FloatWritable(result);
    }

    public int getFirst() {
        return first.get();
    }

    public void setFirst(int first) {
        this.first.set(first);
    }

    public int getSecond() {
        return second.get();
    }

    public void setSecond(int second) {
        this.second.set(second);
    }

    public float getResult() {
        return result.get();
    }
    public void setResult(float result) {
        this.result.set(result);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        result.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        result.readFields(in);
    }

    @Override
    public String toString() {
        return first.get() + "," + second.get() + "," + result.get();
    }
}