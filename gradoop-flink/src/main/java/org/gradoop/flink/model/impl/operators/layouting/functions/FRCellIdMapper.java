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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * A map-function that assigns a cellid to each input-vertex, depending on its position in the
 * layouting-space.
 * The cellid is stored as a property in FRLayouter.CELLID_PROPERTY
 */
public class FRCellIdMapper implements MapFunction<Vertex, Vertex> {
  /** Size of subcells (width and height) */
  private int cellSize;

  /** Create new CellIdMapper
   * @param cellSize Size of subcells (width and height)
   */
  public FRCellIdMapper(int cellSize) {
    this.cellSize = cellSize;
  }

  @Override
  public Vertex map(Vertex value) {
    Vector pos = Vector.fromVertexPosition(value);
    int xcell = ((int) pos.getX()) / cellSize;
    int ycell = ((int) pos.getY()) / cellSize;
    int cellid = (xcell << 16) | ycell;
    value.setProperty(FRLayouter.CELLID_PROPERTY, cellid);
    return value;
  }
}
