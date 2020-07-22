package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.Random;

public class RandomCentroidsMapper extends Mapper<Object, Text, IntWritable, Text> {    
    private final IntWritable randomKey = new IntWritable();
    private final Text point = new Text();
   
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Random random = new Random();
      int rand = random.nextInt();

      randomKey.set(rand);
      point.set(value.toString());

      context.write(randomKey, point);
    }
  }