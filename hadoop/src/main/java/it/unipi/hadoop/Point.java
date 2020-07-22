package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements WritableComparable<Centroid> {
  private List<DoubleWritable> coordinates;

  Point() {
    this.coordinates = new ArrayList<DoubleWritable>();
  }

  Point(final int n) {
    this.coordinates = new ArrayList<DoubleWritable>();

    for (int i = 0; i < n; i++) {
      this.coordinates.add(new DoubleWritable(0.0));
    }
  }

  Point(final List<DoubleWritable> coordinatesList) {
    this.coordinates = new ArrayList<DoubleWritable>();

    for (final DoubleWritable element : coordinatesList) {
        this.coordinates.add(new DoubleWritable(element.get()));
    }
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.coordinates.size());

    for (final DoubleWritable value : this.coordinates) {
        out.writeDouble(value.get());
    }
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    final int size = in.readInt();
    this.coordinates = new ArrayList<DoubleWritable>();

    for (int i = 0; i < size; i++) {
        this.coordinates.add(new DoubleWritable(in.readDouble()));
    }
  }

  @Override
  public String toString() {
    String elements = "";

    for (final DoubleWritable element : this.coordinates) {
        elements += element.get() + ";";
    }

    return elements;
  }

  @Override
  public int compareTo(Centroid o) {
    return 0;
  }

  List<DoubleWritable> getCoordinates() {
    return this.coordinates;
  }

  void setCoordinates(final List<DoubleWritable> newCoordinates) {
    this.coordinates = new ArrayList<DoubleWritable>();

    for (final DoubleWritable value : newCoordinates) {
        this.coordinates.add(new DoubleWritable(value.get()));
    }
  }

}
