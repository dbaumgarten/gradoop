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

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Lightweight verison of Edge. Contains only data necessary for layouting.
 */
public class LEdge extends Tuple4<GradoopId, GradoopId, GradoopId, Integer> implements
  GraphElement {

  /**
   * Position of the ID-property in the tuple
   */
  public static final int ID = 0;
  /**
   * Position of the sourceId-property in the tuple
   */
  public static final int SOURCE_ID = 1;
  /**
   * Position of the targetId-property in the tuple
   */
  public static final int TARGET_ID = 2;

  /**
   * Create LEdge from raw data
   *
   * @param id       Edge-id
   * @param sourceId id of source vertex
   * @param targetId id of target vertex
   * @param count    number of sub-edges contained in this edge
   */
  public LEdge(GradoopId id, GradoopId sourceId, GradoopId targetId, int count) {
    this.f0 = id;
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = count;
  }

  /**
   * Construct LEdge from rgular edge
   *
   * @param e The original edge to copy values from
   */
  public LEdge(Edge e) {
    super(e.getId(), e.getSourceId(), e.getTargetId(), 1);
  }

  /**
   * Default constructor. Needed for POJOs
   */
  public LEdge() {
    super();
    f3 = 1;
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
   * Gets sourceId
   *
   * @return value of sourceId
   */
  public GradoopId getSourceId() {
    return f1;
  }

  /**
   * Sets sourceId
   *
   * @param sourceId the new value
   */
  public void setSourceId(GradoopId sourceId) {
    this.f1 = sourceId;
  }

  /**
   * Gets targetId
   *
   * @return value of targetId
   */
  public GradoopId getTargetId() {
    return f2;
  }

  /**
   * Sets targetId
   *
   * @param targetId the new value
   */
  public void setTargetId(GradoopId targetId) {
    this.f2 = targetId;
  }

  public int getCount() {
    return f3;
  }

  public void setCount(int c) {
    f3 = c;
  }
}
