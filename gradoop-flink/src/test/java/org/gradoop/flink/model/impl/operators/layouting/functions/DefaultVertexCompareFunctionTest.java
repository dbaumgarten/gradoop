package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

public class DefaultVertexCompareFunctionTest {

  @Test
  public void testCompare() {
    DefaultVertexCompareFunction cf = new DefaultVertexCompareFunction(10);

    LVertex v1 = new LVertex(null, new Vector(10, 10), -1, 1, new Vector(5, 0));
    LVertex v2 = new LVertex(null, new Vector(10, 10), -1, 1, new Vector(-5, 0));
    LVertex v3 = new LVertex(null, new Vector(10, 10), -1, 1, new Vector(6, 0));
    Assert.assertEquals(1, cf.compare(v1, v1), 0.0000001);
    Assert.assertEquals(0, cf.compare(v1, v2), 0.0000001);
    Assert.assertTrue(cf.compare(v1, v3) > 0.5);

    LVertex v4 = new LVertex(null, new Vector(10, 20), -1, 1, new Vector(5, 0));
    LVertex v5 = new LVertex(null, new Vector(10, 30), -1, 1, new Vector(5, 0));
    LVertex v6 = new LVertex(null, new Vector(10, 15), -1, 1, new Vector(5, 0));
    Assert.assertEquals(1, cf.compare(v1, v4), 0.0000001);
    Assert.assertEquals(1, cf.compare(v1, v6), 0.0000001);
    Assert.assertEquals(0, cf.compare(v1, v5), 0.0000001);

    LVertex v7 = new LVertex(null, new Vector(10, 10), -1, 10, new Vector(50, 0));
    Assert.assertEquals(1, cf.compare(v7, v7), 0.0000001);
    Assert.assertEquals(0, cf.compare(v7, v2), 0.0000001);
    Assert.assertTrue(cf.compare(v7, v3) > 0.5);
  }
}