package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

import java.io.Serializable;

public interface VertexCompareFunction extends Serializable {
  double compare(LVertex v1, LVertex v2);
}
