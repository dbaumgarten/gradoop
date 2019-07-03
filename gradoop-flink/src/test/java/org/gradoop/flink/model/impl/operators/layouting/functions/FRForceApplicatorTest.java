package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

public class FRForceApplicatorTest {
  @Test
  public void testForceAppplicator() {
    FRForceApplicator fa = new FRForceApplicator(1000, 1000, 10, 25);
    Assert.assertEquals(707.1, fa.speedForIteration(0), 0.1);
    Assert.assertEquals(537.96, fa.speedForIteration(1), 0.1);
    Assert.assertEquals(1.0, fa.speedForIteration(24), 0.1);

    Vector pos = new Vector(950, 0);
    Vector force = new Vector(0, 300);
    fa.applyForce(pos, force, 200);
    Assert.assertEquals(pos, new Vector(950, 200));

    Vector force2 = new Vector(1000, 1000);
    fa.applyForce(pos, force2, 10000);
    Assert.assertEquals(pos, new Vector(999, 999));
  }

}
