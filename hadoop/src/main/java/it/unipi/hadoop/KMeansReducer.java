package it.unipi.hadoop;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;


public class KMeansReducer extends Reducer<Centroid, Point, NullWritable, Text> {
    private Text result = new Text("");
    private static int dimension;
    private static double threshold;
    private final List<Centroid> centroids = new ArrayList<Centroid>();
    public static enum Counter {
      CONVERGED_COUNT
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        dimension = Integer.parseInt(conf.get("dimension"));
        threshold = Double.parseDouble(conf.get("threshold"));
    }

    @Override
    public void reduce(Centroid key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
      Centroid meanCentroid = new Centroid(dimension);
      long numElements = 0;
      
      for (Point currentPoint : values) {
        meanCentroid.add(currentPoint);
        numElements++;
      }

      meanCentroid.setId(key.getId());
      meanCentroid.calculateMean(numElements);
      
      Centroid copy = Centroid.copy(meanCentroid);
      centroids.add(copy);
      Double distance = key.findEuclideanDistance((Point) meanCentroid);

      result.set(meanCentroid.toString() + " - " + copy.toString() + " - Distance: " + distance);
      context.write(null, result);

      if (distance <= threshold) {
        context.getCounter(Counter.CONVERGED_COUNT).increment(1);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);

      Configuration conf = context.getConfiguration();
      Path centersPath = new Path(conf.get("centroidsFilename"));
      FileSystem fs = FileSystem.get(conf);

      if (fs.exists(centersPath)) {
        System.out.println("Delete old output folder: " + centersPath.toString());
        fs.delete(centersPath, true);
      }

      SequenceFile.Writer centroidWriter = SequenceFile.createWriter(conf,
          SequenceFile.Writer.file(centersPath),
          SequenceFile.Writer.keyClass(IntWritable.class),
          SequenceFile.Writer.valueClass(Centroid.class));
     
      for (Centroid c : centroids) {
        centroidWriter.append(c.getId(), c); 
      }

      centroidWriter.close();
    }
  }