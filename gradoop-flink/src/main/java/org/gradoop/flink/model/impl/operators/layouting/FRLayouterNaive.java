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

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

/**
 * Performs a naive version of the RF-Algorithm by using the cartesian product between vertices
 * to compute repulsive-forces.
 * NOT INTENDED FOR PRACTICAL USE. Intended for performance-comparisons
 */
public class FRLayouterNaive extends FRLayouter {
  /** Create new FRLayouterNaive
   *  @param width Width of the layouting-space
   * @param height Height of the layouting-space
   * @param iterations Number of iterations to perform
   * @param k Parameter for the FR-Algorithm. optimum distance between connected vertices
   */
  public FRLayouterNaive(int width, int height, int iterations, double k) {
    super(width, height, iterations, k, 1);
  }

  @Override
  public DataSet<Force> repulsionForces(DataSet<LVertex> vertices) {
    return vertices.cross(vertices).with(new FRRepulsionFunction(k));
  }
}
