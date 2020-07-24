package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class RandomCentroidsReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    static int DIMENSION;
    static int K;
    static int counter = 0;
    private final List<Centroid> centroidsList = new ArrayList<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      DIMENSION = Integer.parseInt(conf.get("dimension"));
      K = Integer.parseInt(conf.get("k"));
    }

    @Override
    // We receive id as IntWritable and values as Iterable Text
    public void reduce(IntWritable randomInteger, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text point : values) {
        if (counter < K) {
          int coordinatesCounter = 0;
          List<DoubleWritable> coordinates = new ArrayList<DoubleWritable>();
          StringTokenizer itr = new StringTokenizer(point.toString(), ",");

          while (itr.hasMoreTokens()) {
            double currentValue = Double.parseDouble(itr.nextToken());

            coordinates.add(new DoubleWritable(currentValue));
            coordinatesCounter++;
            
            if (coordinatesCounter == DIMENSION)
              break;
          }

          Centroid centroid = new Centroid(new IntWritable(counter), coordinates);
          
          centroidsList.add(centroid);
          counter++;


          context.write(null, point);
        }
      }
   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
     super.cleanup(context);

     Configuration conf = context.getConfiguration();
     Path outPath = new Path(conf.get("centroidsFilename"));
     FileSystem fs = FileSystem.get(conf);

     if (fs.exists(outPath)) {
       System.out.println("Delete old output folder: " + outPath.toString());
       fs.delete(outPath, true);
     }

     SequenceFile.Writer writer = SequenceFile.createWriter(conf,
               SequenceFile.Writer.file(outPath),
               SequenceFile.Writer.keyClass(IntWritable.class),
               SequenceFile.Writer.valueClass(Centroid.class));

     for (Centroid centroid : centroidsList) {
       writer.append(new IntWritable(centroid.getId().get()), centroid);
     }

     writer.close();
   }
}