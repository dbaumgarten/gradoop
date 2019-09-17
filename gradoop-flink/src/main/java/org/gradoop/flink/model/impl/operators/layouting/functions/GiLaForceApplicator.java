package org.gradoop.flink.model.impl.operators.layouting.functions;

public class GiLaForceApplicator extends FRForceApplicator {

  private double factor;

  public GiLaForceApplicator(int width, int height, double k, int numVertices) {
    super(width, height, k, 0);
    factor = Math.sqrt((numVertices / (width / height)) * k);
  }

  @Override
  public double speedForIteration(int iteration) {
    if (iteration != lastIteration) {
      lastSpeedLimit = factor * Math.pow(0.93, iteration);
    }
    return lastSpeedLimit;
  }
}
