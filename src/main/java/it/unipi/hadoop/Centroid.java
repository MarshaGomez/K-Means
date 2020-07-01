package it.unipi.hadoop;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;

public class Centroid implements Writable, Comparable<Centroid> {
  private long id;

  @Override
  public void write(DataOutput out) throws IOException
  {
      // 1. Generate the centroids file
      // 2. How to use the centroids file
      // 3. How to hundle the double array. (Every point is goint to be a double array points) Investigate manage the doubles
      // out.writeLong(this.timestamp );
      // out.writeDouble(this.value );
  }

  @Override
  public int compareTo(TimeSeriesData that)
  {
    //   if (this == that)
    //       return 0;
    //   if (this.timestamp  < that.timestamp )
    //       return -1;
    //   if (this.timestamp  > that.timestamp )
    //       return 1;
    //   return 0;
  }

  @Override
  public String toString()
  {
      // return "(" + timestamp + ", " + value + ")";
  }
}
