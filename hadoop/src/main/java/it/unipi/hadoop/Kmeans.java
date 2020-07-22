package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.NullWritable;

import java.util.ArrayList;
import java.util.List;

public class Kmeans {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 6) {
      System.out.println("=======================");
      System.err.println("Usage: kmeans <input> <k> <dimension> <threshold> <centroidsFilename> <output>");
      System.out.println("=======================");
      System.exit(2);
    }
    System.out.println("=======================");
    System.out.println("args[0]: <input>=" + otherArgs[0]);
    System.out.println("=======================");
    System.out.println("args[1]: <k>=" + otherArgs[1]);
    System.out.println("=======================");
    System.out.println("args[2]: <dimension>=" + otherArgs[2]);
    System.out.println("=======================");
    System.out.println("args[3]: <threshold>=" + otherArgs[3]);
    System.out.println("=======================");
    System.out.println("args[4]: <centroidsFilename>=" + otherArgs[4]);
    System.out.println("=======================");
    System.out.println("args[5]: <output>=" + otherArgs[5]);
    System.out.println("=======================");

    long start = System.currentTimeMillis();

    //without randomly centroids comment from 57 to 75, 85, from 178 to 182
    
    Job centroidsJob = Job.getInstance(conf, "RandomCentroid JAVA");

    centroidsJob.setJarByClass(Kmeans.class);
    centroidsJob.setMapperClass(RandomCentroidsMapper.class);
    centroidsJob.setReducerClass(RandomCentroidsReducer.class);
    centroidsJob.setMapOutputKeyClass(IntWritable.class);
    centroidsJob.setMapOutputValueClass(Text.class);
    centroidsJob.setOutputKeyClass(Text.class);
    centroidsJob.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(centroidsJob, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(centroidsJob, new Path(otherArgs[5]));

    centroidsJob.setInputFormatClass(TextInputFormat.class);
    centroidsJob.setOutputFormatClass(TextOutputFormat.class);

    centroidsJob.getConfiguration().set("k", otherArgs[1]);
    centroidsJob.getConfiguration().set("dimension", otherArgs[2]);
    centroidsJob.getConfiguration().set("centroidsFilename", otherArgs[4]);

    Path output = new Path(otherArgs[5]);
    FileSystem fs = FileSystem.get(output.toUri(),conf);
    
    if (fs.exists(output)) {
      System.out.println("Delete old output folder: " + output.toString());
      fs.delete(output, true);
    }

    centroidsJob.waitForCompletion(true);

    // Path centroidsOutput = new Path(otherArgs[4]);
    // FileSystem fs = FileSystem.get(output.toUri(),conf);
    
    // if (fs.exists(centroidsOutput)) {
    //   System.out.println("Delete old output folder: " + output.toString());
    //   fs.delete(centroidsOutput, true);
    // }

    //createCentroids(conf, new Path(otherArgs[4]), Integer.parseInt(otherArgs[2]));

    System.out.println("=======================");
    System.out.println("FIRST CENTROIDS");
    System.out.println("=======================");
    readCentroids(conf, new Path(otherArgs[4]));

    long convergedCentroids = 0;
    
    int k = Integer.parseInt(args[1]);
    long iterations = 0;

    while (convergedCentroids < k) {
      System.out.println("=======================");
      System.out.println("    ITERATION:    " + (iterations + 1));
      System.out.println("    CONVERGED CENTROIDS:    " + convergedCentroids);
      System.out.println("=======================");
      
      if (fs.exists(output)) {
        System.out.println("=======================");
        System.out.println("DELETE OLD OUTPUT FOLDER: " + output.toString());
        System.out.println("=======================");
        fs.delete(output, true);
      }

      Job job = Job.getInstance(conf, "Kmean JAVA");
      
      job.getConfiguration().set("k", otherArgs[1]);
      job.getConfiguration().set("dimension", otherArgs[2]);
      job.getConfiguration().set("threshold", otherArgs[3]);
      job.getConfiguration().set("centroidsFilename", otherArgs[4]);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setJarByClass(Kmeans.class);
      job.setMapperClass(KMeansMapper.class);
      job.setReducerClass(KMeansReducer.class);
      job.setMapOutputKeyClass(Centroid.class);
      job.setMapOutputValueClass(Point.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));

      job.waitForCompletion(true);
      
      convergedCentroids = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED_COUNT).getValue();
      iterations++;
    }
    
    long end = System.currentTimeMillis();
    long elapsedTime = end - start;
    long minutes = (elapsedTime / 1000) / 60;
    long seconds = (elapsedTime / 1000) % 60;
    
    System.out.println("=======================");
    System.out.println(" TOTAL TIME " + minutes + " m " + seconds + "s");
    System.out.println("=======================");
    System.out.println("::FINAL CENTROIDS::");
    System.out.println("=======================");
    System.out.println("::NUMBER OF ITERATIONS:: " + iterations);
    System.out.println("=======================");
    System.out.println("::NUMBER OF CONVERGED COUNT:: " + convergedCentroids);
    System.out.println("=======================");
    
    
    readCentroids(conf, new Path(otherArgs[4]));
  }


  private static void createCentroids(Configuration conf, Path centroids, int dimension) throws IOException {
    SequenceFile.Writer centroidWriter = SequenceFile.createWriter(conf,
            SequenceFile.Writer.file(centroids),
            SequenceFile.Writer.keyClass(IntWritable.class),
            SequenceFile.Writer.valueClass(Centroid.class));

    List<DoubleWritable> listParameters = new ArrayList<DoubleWritable>();
    Centroid auxiliarCentroid;

    
    
    double[][] arrays = {
      {0.297959,0.469496,0.211699,0.077399,0.256267,0.08078,0.169916,0.0625,0.670639},
      {0.297959,0.469496,0.220056,0.074303,0.247911,0.072423,0.172702,0.0625,0.703142},
      {0.297959,0.469496,0.239554,0.06192,0.256267,0.064067,0.208914,0.05625,0.698808},
    };
   
      //d = 3 k = 7
      //double[][] arrays = {{0.297959,0.474801,0.448468}, {0.297959,0.480106,0.214485}, {0.297959,0.482759,0.247911}, {0.297959,0.482759,0.51532}, {0.297959,0.469496,0.211699}, {0.297959,0.458886,0.220056}, {0.297959,0.453581,0.239554}};
      //d = 3 k = 13
      //double[][] arrays = {{0.297959,0.474801,0.448468}, {0.297959,0.480106,0.214485}, {0.297959,0.482759,0.247911}, {0.297959,0.482759,0.51532}, {0.297959,0.469496,0.211699}, {0.297959,0.458886,0.220056}, {0.297959,0.453581,0.239554},{0.297959,0.450928,0.292479}, {0.297959,0.450928,0.259053}, {0.297959,0.450928,0.401114}, {0.293878,0.464191,0.292479}, {0.293878,0.477454,0.32312}, {0.293878,0.482759,0.395543}};
      //d = 7 k = 7
      //double[][] arrays = {{0.297959,0.474801,0.448468,0.024768,0.598886,0.038997,0.119777}, {0.297959,0.480106,0.214485,0.021672,0.398329,0.030641,0.902507}, {0.297959,0.482759,0.247911, 0.037152,0.311978,0.041783,0.033426}, {0.297959,0.482759,0.51532,0.012384,0.724234,0.02507,0.278552}, {0.297959,0.469496,0.211699,0.077399,0.256267,0.08078,0.169916}, {0.297959,0.458886,0.220056,0.074303,0.247911,0.072423,0.172702}, {0.297959,0.453581,0.239554,0.06192,0.256267,0.064067,0.208914}};
      //d = 7 k = 13
     // double[][] arrays = {{0.297959,0.474801,0.448468,0.024768,0.598886,0.038997,0.119777}, {0.297959,0.480106,0.214485,0.021672,0.398329,0.030641,0.902507}, {0.297959,0.482759,0.247911,0.037152,0.311978,0.041783,0.033426}, {0.297959,0.482759,0.51532,0.012384,0.724234,0.02507,0.278552}, {0.297959,0.469496,0.211699,0.077399,0.256267,0.08078,0.169916}, {0.297959,0.458886,0.220056,0.074303,0.247911,0.072423,0.172702}, {0.297959,0.453581,0.239554,0.06192,0.256267,0.064067,0.208914},{0.297959,0.450928,0.292479,0.043344,0.348189,0.050139,0.228412}, {0.297959,0.450928,0.259053,0.012384,0.350975,0.016713,0.038997}, {0.297959,0.450928,0.401114,0.037152,0.465181,0.047354,0.320334}, {0.293878,0.464191,0.292479,0.049536,0.350975,0.052925,0.256267}, {0.293878,0.477454,0.32312,0.055728,0.398329,0.072423,0.289694}, {0.293878,0.482759,0.395543,0.034056,0.557103,0.050139,0.259053}};

    for (int i = 0; i < arrays.length; i++) {
        for (int j = 0; j < dimension; j++) {
          listParameters.add(new DoubleWritable(arrays[i][j]));
        }

        auxiliarCentroid = new Centroid(new IntWritable(i), listParameters);
        centroidWriter.append(new IntWritable(i), auxiliarCentroid);
        listParameters = new ArrayList<DoubleWritable>();
    }

    centroidWriter.close();
  }

  private static void readCentroids(Configuration conf, Path centroids) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centroids));
        IntWritable key = new IntWritable();
        Centroid value = new Centroid();

        while (reader.next(key, value)) {
          System.out.println(value);
        }

        reader.close();
  }
}

// mvn clean package
// hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans data.txt 7 3 0.5 centroids.txt output
// cat /opt/yarn/logs/
// hadoop fs -cat output/part* | head


//THRESHOLD 0.0001
//test point n = 1.000
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-1k.txt 7 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-1k.txt 13 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-1k.txt 7 7 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-1k.txt 13 7 0.0001 centroids.txt output 

//test point n = 10.000
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-10k.txt 7 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-10k.txt 13 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-10k.txt 7 7 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-10k.txt 13 7 0.0001 centroids.txt output 


//test point n = 100.000
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-100k.txt 7 3 0.0001 centroids.txt output
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-100k.txt 13 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-100k.txt 7 7 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-100k.txt 13 7 0.0001 centroids.txt output 
