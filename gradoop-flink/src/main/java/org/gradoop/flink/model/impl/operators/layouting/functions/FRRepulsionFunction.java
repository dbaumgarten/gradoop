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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.Random;

/**
 * A Join/Cross/FlatJoin-Function that computes the repulsion-forces between two given vertices.
 * Implements both Join and FlatJoin to compute repulsions for a single vertex or for both
 * vertices at once.
 */
public class FRRepulsionFunction implements
  JoinFunction<LVertex, LVertex, Force>,
  CrossFunction<LVertex, LVertex, Force>,
  FlatJoinFunction<LVertex,LVertex,Force> {
  /** Rng. Used to get random directions for vertices at the same position */
  private Random rng;
  /** Parameter for the FR-Algorithm */
  private double k;
  /** Maximum distance between two vertices that still produces a repulsion */
  private double maxDistance;

  /** Create new RepulsionFunction
   *
   * @param k A parameter of the FR-Algorithm
   */
  public FRRepulsionFunction(double k) {
    this(k,Float.MAX_VALUE);
  }

  /** Create new RepulsionFunction
   *
   * @param k A parameter of the FR-Algorithm
   * @param maxDistance Maximum distance between two vertices that still produces a repulsion
   */
  public FRRepulsionFunction(double k, double maxDistance) {
    rng = new Random();
    this.k = k;
    this.maxDistance = maxDistance;
  }

  /** Computes the repulsion force between two vertices ONLY FOR THE FIRST vertex
   *
   * @param first First Vertex
   * @param second Second Certex
   * @return A force-tuple representing the repulsion-force for the first vertex
   */
  @Override
  public Force join(LVertex first, LVertex second) {
    Vector force = calculateForce(first,second);
    return new Force(first.getId(),force);
  }

  /** Alias for join() to fullfill the CrossFunction-Interface.
   *
   * @param vertex First Vertex
   * @param vertex2 Second Certex
   * @return A force-tuple representing the repulsion-force for the first vertex
   */
  @Override
  public Force cross(LVertex vertex, LVertex vertex2) {
    return join(vertex, vertex2);
  }


  /** Computes the repulsion force between two vertices ONLY FOR THE FIRST vertex
   *
   * @param first First Vertex
   * @param second Second Certex
   * @return A force-tuple representing the repulsion-force for the first vertex
   */
  protected Vector calculateForce(LVertex first, LVertex second){
    Vector pos1 = first.getPosition();
    Vector pos2 = second.getPosition();
    double distance = pos1.distance(pos2);
    Vector direction = pos2.sub(pos1);

    if (first.getId().equals(second.getId())) {
      return new Vector(0,0);
    }

    if (distance > maxDistance){
      return new Vector(0,0);
    }

    if (distance == 0) {
      distance = 0.1;
      direction.setX(rng.nextInt());
      direction.setY(rng.nextInt());
    }

    return direction.normalized().mul(-Math.pow(k, 2) / distance);
  }

  /** Implement FlatJoin and produce the force-tuples for both vertices at once.
   * (Forces of magnitude 0 will be ignored)
   *
   * @param first The first vertex
   * @param second The second vertex
   * @param collector Contains up to two force-tuples representing repulsion-forces between both
   *                  vertices.
   */
  @Override
  public void join(LVertex first, LVertex second,
    Collector<Force> collector){

    Vector force = calculateForce(first,second);
    if (force.magnitude() == 0){
      return;
    }
    collector.collect(new Force(first.getId(),force));
    collector.collect(new Force(second.getId(),force.mul(-1)));
  }
}
