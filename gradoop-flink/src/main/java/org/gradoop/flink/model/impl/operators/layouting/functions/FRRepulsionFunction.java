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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.Random;

/**
 * A JoinFunction that computes the repulsion-forces between two given vertices.
 */
public class FRRepulsionFunction implements
  JoinFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>>,
  CrossFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>> {
  /** Rng. Used to get random directions for vertices at the same position */
  private Random rng;
  /** Parameter for the FR-Algorithm */
  private double k;

  /** Create new RepulsionFunction
   *
   * @param k A parameter of the FR-Algorithm
   */
  public FRRepulsionFunction(double k) {
    rng = new Random();
    this.k = k;
  }

  @Override
  public Tuple3<GradoopId, Double, Double> join(Vertex first, Vertex second) {
    Vector pos1 = Vector.fromVertexPosition(first);
    Vector pos2 = Vector.fromVertexPosition(second);
    double distance = pos1.distance(pos2);
    Vector direction = pos2.sub(pos1);

    if (first.getId().equals(second.getId())) {
      return new Tuple3<GradoopId, Double, Double>(first.getId(), 0.0, 0.0);
    }
    if (distance == 0) {
      distance = 0.1;
      direction.setX(rng.nextInt());
      direction.setY(rng.nextInt());
    }

    Vector force = direction.normalized().mul(-Math.pow(k, 2) / distance);

    return new Tuple3<GradoopId, Double, Double>(first.getId(), force.getX(), force.getY());
  }

  @Override
  public Tuple3<GradoopId, Double, Double> cross(Vertex vertex, Vertex vertex2) throws Exception {
    return join(vertex, vertex2);
  }
}
