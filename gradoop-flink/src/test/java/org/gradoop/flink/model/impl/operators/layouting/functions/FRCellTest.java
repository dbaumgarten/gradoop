package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.junit.Assert;
import org.junit.Test;

import static org.gradoop.flink.model.impl.operators.layouting.functions.Util.getDummyVertex;

public class FRCellTest {

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
    Assert.assertEquals(id(1, 0), (long) neighborslector.getKey(getDummyVertex(id(0, 0))));
    Assert.assertEquals(id(6, 3), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(10, 9), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.LEFT);
    Assert.assertEquals(id(-1, 0), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4, 3), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(8, 9), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UP);
    Assert.assertEquals(id(0, -1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(5, 2), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(9, 8), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWN);
    Assert.assertEquals(id(0, 1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(5, 4), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(9, 10), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UPRIGHT);
    Assert.assertEquals(id(1, -1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(6, 2), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(10, 8), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.UPLEFT);
    Assert.assertEquals(id(-1, -1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4, 2), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(8, 8), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWNLEFT);
    Assert.assertEquals(id(-1, 1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(4, 4), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(8, 10), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

    neighborslector = new FRCellIdSelector(FRCellIdSelector.NeighborType.DOWNRIGHT);
    Assert.assertEquals(id(1, 1), (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(id(6, 4), (long) neighborslector.getKey(getDummyVertex(id(5, 3))));
    Assert.assertEquals(id(10, 10), (long) neighborslector.getKey(getDummyVertex(id(9, 9))));

  }
}
