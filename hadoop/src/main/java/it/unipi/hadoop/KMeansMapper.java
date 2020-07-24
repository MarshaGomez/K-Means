package it.unipi.hadoop;

import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

public class KMeansMapper extends Mapper<Object, Text, Centroid, Point> {
  private final List<Centroid> centroids = new ArrayList<>();
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private int configurationDimension;
  private final Point point = new Point();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      Path centersPath = new Path(conf.get("centroidsFilename"));
      SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersPath));
      IntWritable key = new IntWritable();
      Centroid value = new Centroid();
      configurationDimension = Integer.parseInt(conf.get("dimension"));
      
      while (reader.next(key, value)) {
          Centroid c = new Centroid(key, value.getCoordinates());

          centroids.add(c);
      }

      reader.close();
  }

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString(), ",");
    List<DoubleWritable> pointsList = new ArrayList<DoubleWritable>();
    int count = 0;

    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      Double coordinate = Double.valueOf(word.toString());
      pointsList.add(new DoubleWritable(coordinate));
      count = count + 1;
      

      if (count == configurationDimension) {
        break;
      }
    }
    point.setCoordinates(pointsList);
    Centroid closestCentroid = null;
    Double minimumDistance = Double.MAX_VALUE;
    for (Centroid c1 : centroids) {
      Double distance = c1.findEuclideanDistance(point);

      if (distance < minimumDistance) {
        minimumDistance = distance;
        closestCentroid = Centroid.copy(c1);
      }
    }

    context.write(closestCentroid, point);
  }

}