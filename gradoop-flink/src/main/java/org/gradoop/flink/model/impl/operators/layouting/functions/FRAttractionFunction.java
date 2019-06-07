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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Computes Attraction forces between two Vertices.
 * Input: two vertices that are connected by an edge.
 * Output: two force-tuples (one for each vertex) representing the attraction-forces between the
 * vertices.
 */
public class FRAttractionFunction implements
  FlatMapFunction<Tuple2<LVertex,LVertex>,Force> {
  /** Parameter for the FR-Algorithm */
  private double k;

  /** Object reuse for output */
  private Force firstForce = new Force();
  /** Object reuse for output */
  private Force secondForce = new Force();
  /** Object reuse for output */
  private Vector force = new Vector();
  /** Object reuse */
  private Vector force2 = new Vector();


  /** Create new FRAttractionFunction
   *
   * @param k Algorithm factor. Optimum distance between connected vertices
   */
  public FRAttractionFunction(double k) {
    this.k = k;
  }


  @Override
  public void flatMap(Tuple2<LVertex,LVertex> vertices,
    Collector<Force> collector) throws
    Exception {
    Vector pos1 = vertices.f0.getPosition();
    Vector pos2 = vertices.f1.getPosition();
    double distance = pos1.distance(pos2);

    force.set(pos2.mSub(pos1).mNormalized().mMul(Math.pow(distance, 2) / k));
    force2.set(force).mMul(-1);

    firstForce.set(vertices.f0.getId(),force);
    secondForce.set(vertices.f1.getId(),force2);

    collector.collect(firstForce);
    collector.collect(secondForce);
  }
}
