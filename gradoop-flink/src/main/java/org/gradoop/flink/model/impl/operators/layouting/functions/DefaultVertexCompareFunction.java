package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

public class DefaultVertexCompareFunction implements VertexCompareFunction {
  protected double k;

  public DefaultVertexCompareFunction(double k) {
    this.k = k;
  }

  @Override
  public double compare(LVertex v1, LVertex v2) {
    double positionSimilarity =
      Math.min(1,
        Math.max(0,
          1-((v1.getPosition().distance(v2.getPosition())-k)/k)));

    Vector force1 = v1.getForce().mDiv(v1.getCount());
    Vector force2 = v2.getForce().mDiv(v2.getCount());
    double forceSimilarity =
        1-(force1.distance(force2)/(force1.magnitude()+force2.magnitude()));

    return positionSimilarity*forceSimilarity;
  }
}
