package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

public class Util {
  static LVertex getDummyVertex(int cellid) {
    LVertex v = new LVertex();
    v.setCellid(cellid);
    return v;
  }

  static LVertex getDummyVertex(int x, int y) throws Exception {
    LVertex v = new LVertex(GradoopId.get(), new Vector(x, y));
    return v;
  }
}
