package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.collections.comparators.NullComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.SequenceFile;

public class Kmeans {

  public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path centersPath = new Path(conf.get("centroidsFilename"));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersPath));
        IntWritable key = new IntWritable();
        // Center value = new Center();

        while (reader.next(key, value)) {
            // Instead to read form the file, make a vector
            // Center c = new Center(value.getListOfCoordinates());
            // c.setNumberOfPoints(new IntWritable(0));
            // c.setIndex(key);
            // centers.add(c);
        }

        reader.close();
        // logger.fatal("Centers: " + centers.toString());
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(new Text("key"), word);
      }
    }
  }

  public static class KMeansReducer extends Reducer<Text, Text, NullWritable, Text> {
    private Text result = new Text("");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String results = "";

      for (Text val : values) {
        results = results + " " + val.toString();
      }

      result.set(results);
      context.write(null, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 6) {
      System.err.println("Usage: kmeans <input> <k> <dimension> <threshold> <centroidsFilename> <output>");
      System.exit(2);
    }

    System.out.println("args[0]: <input>=" + otherArgs[0]);
    System.out.println("args[1]: <k>=" + otherArgs[1]);
    System.out.println("args[2]: <dimension>=" + otherArgs[2]);
    System.out.println("args[3]: <threshold>=" + otherArgs[3]);
    System.out.println("args[5]: <output>=" + otherArgs[4]);
    System.out.println("args[6]: <centroidsFilename>=" + otherArgs[5]);

    Job job = Job.getInstance(conf, "kmean");
    job.getConfiguration().set("k", otherArgs[1]);
    job.getConfiguration().set("dimension", otherArgs[2]);
    job.getConfiguration().set("threshold", otherArgs[3]);
    job.getConfiguration().set("centroidsFilename", otherArgs[4]);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setJarByClass(Kmeans.class);
    job.setMapperClass(KMeansMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(KMeansReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
