/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRAttractionFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdMapper;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdSelector;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRForceApplicator;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FRLayouterTest extends LayoutingAlgorithmTest {

  private int id(int x, int y) {
    return (x << 16) | y;
  }

  @Test
  public void testCellIdSelector() throws Exception {
    int cellSize = 10;
    KeySelector<LVertex, Integer> selfselector =
      new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF);
    FRCellIdMapper mapper = new FRCellIdMapper(cellSize);

    Assert.assertEquals(0, (long) selfselector.getKey(mapper.map(getDummyVertex(0, 0))));
    Assert.assertEquals(id(9, 9), (long) selfselector.getKey(mapper.map(getDummyVertex(99, 98))));
    Assert.assertEquals(id(0, 9), (long) selfselector.getKey(mapper.map(getDummyVertex(0, 95))));

    KeySelector<LVertex, Integer> neighborslector =
      new FRCellIdSelector(FRCellIdSelector.NeighborType.RIGHT);
    Assert.assertEquals(id(1,0), (long) neighborslector.getKey(getDummyVertex(id(0,0))));
    Assert.assertEquals(id(6,3), (long) neighborslector.getKey(getDummyVertex(id(5,3))));
    Assert.assertEquals(id(10,9), (long) neighborslector.getKey(getDummyVertex(id(9,9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.LEFT);
    Assert.assertEquals(id(-1,0), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4,3), (long) neighborslector.getKey(getDummyVertex(id(5,3))));
    Assert.assertEquals(id(8,9), (long) neighborslector.getKey(getDummyVertex(id(9,9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UP);
    Assert.assertEquals(id(0,-1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(5,2), (long) neighborslector.getKey(getDummyVertex(id(5,3))));
    Assert.assertEquals(id(9,8), (long) neighborslector.getKey(getDummyVertex(id(9,9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWN);
    Assert.assertEquals(id(0,1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(5,4), (long) neighborslector.getKey(getDummyVertex(id(5,3))));
    Assert.assertEquals(id(9,10), (long) neighborslector.getKey(getDummyVertex(id(9,9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UPRIGHT);
    Assert.assertEquals(id(1,-1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(6,2), (long) neighborslector.getKey(getDummyVertex(id(5,3))));
    Assert.assertEquals(id(10,8), (long) neighborslector.getKey(getDummyVertex(id(9,9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UPLEFT);
    Assert.assertEquals(id(-1,-1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4,2), (long) neighborslector.getKey(getDummyVertex(id(5,3))));
    Assert.assertEquals(id(8,8), (long) neighborslector.getKey(getDummyVertex(id(9,9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWNLEFT);
    Assert.assertEquals(id(-1,1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4,4), (long) neighborslector.getKey(getDummyVertex(id(5,3))));
    Assert.assertEquals(id(8,10), (long) neighborslector.getKey(getDummyVertex(id(9,9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWNRIGHT);
    Assert.assertEquals(id(1,1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(6,4), (long) neighborslector.getKey(getDummyVertex(id(5,3))));
    Assert.assertEquals(id(10,10), (long) neighborslector.getKey(getDummyVertex(id(9,9))));

  }

  @Test
  public void testRepulseJoinFunction() throws Exception {
    JoinFunction<LVertex, LVertex, Force> jf =
      new FRRepulsionFunction(1, 20);
    LVertex v1 = getDummyVertex(1, 1);
    LVertex v2 = getDummyVertex(2, 3);
    LVertex v3 = getDummyVertex(7, 5);
    LVertex v4 = getDummyVertex(1, 1);
    LVertex v5 = getDummyVertex(30, 30);

    Vector vec12 = jf.join(v1, v2).getValue().copy();
    Vector vec13 = jf.join(v1, v3).getValue().copy();
    Vector vec14 = jf.join(v1, v4).getValue().copy();
    Vector vec11 = jf.join(v1, v1).getValue().copy();
    Vector vec15 = jf.join(v1, v5).getValue().copy();

    Assert.assertTrue(vec12.getX() < 0 && vec12.getY() < 0);
    Assert.assertTrue(vec12.magnitude() > vec13.magnitude());
    Assert.assertTrue(vec14.magnitude() > 0);
    Assert.assertTrue(vec11.magnitude() == 0);
    Assert.assertTrue(vec15.magnitude() == 0);
  }

  @Test
  public void testRepulseFlatJoin() throws Exception {
    FRRepulsionFunction jf = new FRRepulsionFunction(1);
    LVertex v1 = getDummyVertex(1, 1);
    LVertex v2 = getDummyVertex(2, 3);

    Vector vec12join = jf.join(v1, v2).getValue().copy();
    v1 = getDummyVertex(1, 1);
    v2 = getDummyVertex(2, 3);

    List<Force> collectorList = new ArrayList<>();
    ListCollector<Force> collector = new ListCollector<>(collectorList);
    jf.join(v1, v2, collector);

    Vector vec12 = collectorList.get(0).getValue().copy();
    Vector vec21 = collectorList.get(1).getValue().copy();

    Assert.assertEquals(vec12join, vec12);
    Assert.assertEquals(vec12, vec21.mul(-1));
    Assert.assertNotEquals(collectorList.get(0).getId(),collectorList.get(1).getId());
  }

  @Test
  public void testAttractionFunction() throws Exception {
    FRAttractionFunction af = new FRAttractionFunction(10);
    LVertex v1 = getDummyVertex(1, 1);
    LVertex v2 = getDummyVertex(2, 3);
    LVertex v3 = getDummyVertex(7, 5);
    LVertex v4 = getDummyVertex(1, 1);

    List<Force> collectorList = new ArrayList<>();
    ListCollector<Force> collector = new ListCollector<>(collectorList);

    af.flatMap(new Tuple2<>(v1, v2), collector);
    Vector vec12 = collectorList.get(0).getValue().copy();
    Vector vec21 = collectorList.get(1).getValue().copy();
    Assert.assertNotEquals(collectorList.get(0).getId(),collectorList.get(1).getId());
    collectorList.clear();

    af.flatMap(new Tuple2<>(v1, v3), collector);
    Vector vec13 = collectorList.get(0).getValue().copy();
    collectorList.clear();

    af.flatMap(new Tuple2<>(v1, v4), collector);
    Vector vec14 = collectorList.get(0).getValue().copy();
    collectorList.clear();

    af.flatMap(new Tuple2<>(v1, v1), collector);
    Vector vec11 = collectorList.get(0).getValue().copy();
    collectorList.clear();


    Assert.assertEquals(vec12, vec21.mul(-1));
    Assert.assertTrue(vec12.getX() > 0 && vec12.getY() > 0);
    Assert.assertTrue(vec12.magnitude() < vec13.magnitude());
    Assert.assertEquals(vec14,new Vector());
    Assert.assertTrue(vec11.magnitude() == 0);
  }

  @Test
  public void testForceAppplicator(){
    FRForceApplicator fa = new FRForceApplicator(1000,1000,10, 25);
    Assert.assertEquals(707.1,fa.speedForIteration(0),0.1);
    Assert.assertEquals(537.96,fa.speedForIteration(1),0.1);
    Assert.assertEquals(1.0,fa.speedForIteration(24),0.1);

    Vector pos = new Vector(950,0);
    Vector force = new Vector(0,300);
    fa.applyForce(pos,force,200);
    Assert.assertEquals(pos,new Vector(950,200));

    Vector force2 = new Vector(1000,1000);
    fa.applyForce(pos,force2,10000);
    Assert.assertEquals(pos,new Vector(999,999));
  }


  private LVertex getDummyVertex(int cellid) {
    LVertex v = new LVertex();
    v.setCellid(cellid);
    return v;
  }

  private LVertex getDummyVertex(int x, int y) throws Exception {
    LVertex v = new LVertex(GradoopId.get(), new Vector(x,y));
    return v;
  }

  @Override
  public LayoutingAlgorithm getLayouter(int w, int h) {
    return new FRLayouter(5,12);
  }
}
