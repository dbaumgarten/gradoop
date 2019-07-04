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
package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Leightweight/Layouting-Vertex. Has all properties of a Vertex that are important for
 * the layouting. This way we do not need to drag around a full Vertex through every operation.
 */
public class LVertex extends Tuple5<GradoopId, Vector, Integer, Integer, Vector> implements
  GraphElement {

  /**
   * Position of the ID-property in the tuple
   */
  public static final int ID = 0;

  /**
   * Create new LVertex
   *
   * @param id       Id of the original vertex
   * @param position Position of the original vertex
   */
  public LVertex(GradoopId id, Vector position) {
    super(id, position, -1, 1, new Vector());
  }

  /**
   * Create new LVertex
   *
   * @param id       Id of the original vertex
   * @param position Position of the original vertex
   * @param cellid   Id of grid-cell this vertex should be assigned to
   */
  public LVertex(GradoopId id, Vector position, int cellid) {
    super(id, position, cellid, 1, new Vector());
  }

  /**
   * Create new LVertex
   *
   * @param id       Id of the original vertex
   * @param position Position of the original vertex
   * @param cellid   Id of grid-cell this vertex should be assigned to
   * @param count    Number of vertices this super-vertex combines
   */
  public LVertex(GradoopId id, Vector position, int cellid, int count) {
    super(id, position, cellid, count, new Vector());
  }

  /**
   * Create new LVertex
   *
   * @param id       Id of the original vertex
   * @param position Position of the original vertex
   * @param cellid   Id of grid-cell this vertex should be assigned to
   * @param count    Number of vertices this super-vertex combines
   * @param force    Last force calculated for this vertex
   */
  public LVertex(GradoopId id, Vector position, int cellid, int count, Vector force) {
    super(id, position, cellid, count, force);
  }

  /**
   * Create new LVertex
   *
   * @param v The original vertex to copy all information from
   */
  public LVertex(Vertex v) {
    super(v.getId(), Vector.fromVertexPosition(v), -1, 1, new Vector());
  }

  /**
   * Default-Constructor to comply with Pojo-Rules
   */
  public LVertex() {
    super(null, new Vector(), -1, 1, new Vector());
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public GradoopId getId() {
    return f0;
  }

  /**
   * Sets id
   *
   * @param id the new value
   */
  public void setId(GradoopId id) {
    this.f0 = id;
  }

  /**
   * Gets position
   *
   * @return value of position
   */
  public Vector getPosition() {
    return f1;
  }

  /**
   * Sets position
   *
   * @param position the new value
   */
  public void setPosition(Vector position) {
    this.f1 = position;
  }

  /**
   * Gets cellid
   *
   * @return value of cellid
   */
  public int getCellid() {
    return f2;
  }

  /**
   * Sets cellid
   *
   * @param cellid the new value
   */
  public void setCellid(int cellid) {
    this.f2 = cellid;
  }

  public int getCount() {
    return f3;
  }

  public void setCount(int count) {
    f3 = count;
  }

  public Vector getForce() {
    return f4;
  }

  public void setForce(Vector v) {
    f4 = v;
  }

}
