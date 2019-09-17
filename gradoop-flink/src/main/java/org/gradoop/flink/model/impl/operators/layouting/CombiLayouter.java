package org.gradoop.flink.model.impl.operators.layouting;

import org.gradoop.flink.model.impl.epgm.LogicalGraph;

public class CombiLayouter implements LayoutingAlgorithm {

  private static final double K_FACTOR = 1.3;

  private int iterations;
  private int numberOfVertices;
  private double quality;
  private CentroidFRLayouter layouter1;
  private FRLayouter layouter2;

  public CombiLayouter(int iterations, int numVertices, double quality) {

    this.iterations = iterations;
    this.numberOfVertices = numVertices;
    this.quality = quality;
    int l1iterations = (int) Math.floor(iterations * (1 - quality));
    int l2iterations = (int) Math.ceil(iterations * (quality));

    if (l1iterations > 0) {
      layouter1 = new CentroidFRLayouter(l1iterations, numVertices);
      layouter1.k(layouter1.getK() * K_FACTOR);
    }
    if (l2iterations > 0) {
      layouter2 = new FRLayouter(l2iterations, numVertices).useExistingLayout(layouter1 != null)
        .startAtIteration(l1iterations);
    }
  }

  public CombiLayouter k(double k) {
    if (layouter1 != null) {
      layouter1.k(k * K_FACTOR);
    }
    if (layouter2 != null) {
      layouter2.k(k);
    }
    return this;
  }


  public CombiLayouter area(int width, int height) {
    if (layouter1 != null) {
      layouter1.area(width, height);
      if (layouter2 != null) {
        layouter2.area(width, height);
      }
    }
    return this;
  }

  public CombiLayouter maxRepulsionDistance(int maxRepulsionDistance) {
    if (layouter2 != null) {
      layouter2.maxRepulsionDistance(maxRepulsionDistance);
    }
    return this;
  }

  public CombiLayouter useExistingLayout(boolean uel) {
    if (layouter1 != null) {
      layouter1.useExistingLayout(uel);
    } else {
      layouter2.useExistingLayout(uel);
    }
    return this;
  }

  public double getK() {
    if (layouter2 != null) {
      return layouter2.getK();
    } else {
      return layouter1.getK() / K_FACTOR;
    }
  }

  public int getMaxRepulsionDistance() {
    if (layouter2 == null) {
      return -1;
    }
    return layouter2.getMaxRepulsionDistance();
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {
    if (layouter1 != null) {
      g = layouter1.execute(g);
    }
    if (layouter2 != null) {
      g = layouter2.execute(g);
    }
    return g;
  }

  @Override
  public int getWidth() {
    if (layouter2 != null) {
      return layouter2.getWidth();
    } else {
      return layouter1.getWidth();
    }
  }

  @Override
  public int getHeight() {
    if (layouter2 != null) {
      return layouter2.getHeight();
    } else {
      return layouter1.getHeight();
    }
  }

  @Override
  public String toString() {
    return "CombiFRLayouter{" + " quality=" + quality + ", iterations=" + iterations + ", k=" +
      getK() + "," + " " + "with=" + getWidth() + ", height=" + getHeight() +
      ", maxRepulsionDistance=" + getMaxRepulsionDistance() + ", numberOfVertices=" +
      numberOfVertices + '}';
  }
}
