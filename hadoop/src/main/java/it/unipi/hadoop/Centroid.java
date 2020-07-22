package it.unipi.hadoop;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class Centroid extends Point {
  private IntWritable id;

  Centroid() {
    super();

    this.id = new IntWritable(-1);
  }

  Centroid(int n) {
    super(n);

    this.id = new IntWritable(-1);
  }

  Centroid(IntWritable id, List<DoubleWritable> coordinates) {
    super(coordinates);

    this.id = new IntWritable(id.get());
  }

  public IntWritable getId() {
    return this.id;
  }

  public void setId(IntWritable newId){
    this.id = new IntWritable(newId.get());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(this.getId().get());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    this.id = new IntWritable(in.readInt());
  }

  @Override
  public String toString() {
    return this.getId().get() + ";" + super.toString();
  }

  @Override
  public int compareTo(Centroid otherCentroid) {
    return Integer.compare(this.getId().get(), otherCentroid.getId().get());
  }

  public static Centroid copy(final Centroid old) {
    return new Centroid(old.getId(), old.getCoordinates());
  }


  public Double findEuclideanDistance(Point point) {
    int lenght = point.getCoordinates().size();
    List<DoubleWritable> pointCoordinates = point.getCoordinates();
    Double sum = 0.0;

    for (int i = 0; i < lenght; i++) {
        Double difference = this.getCoordinates().get(i).get() - pointCoordinates.get(i).get();
        sum += Math.pow(difference, 2);

    }

    return Math.sqrt(sum);
  }
  
  public void add(Point currentPoint) {
    int length = currentPoint.getCoordinates().size();
    List<DoubleWritable> currentPointCoordinates = currentPoint.getCoordinates();

    for(int i = 0; i < length; i++){
      Double centroidCoordinate = this.getCoordinates().get(i).get();
      Double currentPointCoordinate = currentPointCoordinates.get(i).get();
      Double sum = centroidCoordinate + currentPointCoordinate;
      
      this.getCoordinates().set(i, new DoubleWritable(sum));       
    }
  }

  public void calculateMean(long numElements) {
    int length = this.getCoordinates().size();
    
    for(int i = 0; i < length; i++){
      Double centroidCoordinate = this.getCoordinates().get(i).get();
      Double mean = centroidCoordinate / numElements;
      //
      mean = (double) Math.round(mean * 1000000d) / 1000000d;
      //
      this.getCoordinates().set(i, new DoubleWritable(mean));
    }
  }
  
}


