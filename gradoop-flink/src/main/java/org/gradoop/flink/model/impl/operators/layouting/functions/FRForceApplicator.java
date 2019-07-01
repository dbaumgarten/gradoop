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

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Applies forces to vertices.
 * Uses simulated annealing with exponentially decreasing temperature.
 * Confines new coordinates to the layouting-space
 */
public class FRForceApplicator extends RichJoinFunction<LVertex, Force, LVertex> {

  /**
   * Width of the layouting-space
   */
  private int layoutWidth;

  /**
   * Height of the layouting space
   */
  private int layoutHeight;

  /**
   * Speed at which the cooling-schedule starts
   */
  private double startSpeed;

  /**
   * Base of the exponentially-decreasing function for the speed
   */
  private double base;

  /**
   * cache the last computed temperature and re-use if possible to reduce needed computing-power
   */
  private int lastIteration = -1;

  /**
   * cache the last computed temperature and re-use if possible to reduce needed computing-power
   */
  private double lastTemperature = 0;

  /**
   * Create new FRForceApplicator
   *
   * @param width         The width of the layouting-space
   * @param height        The height of the layouting-space
   * @param k             A parameter of the FR-Algorithm.
   * @param maxIterations Number of iterations the FR-Algorithm will have
   */
  public FRForceApplicator(int width, int height, double k, int maxIterations) {
    this.layoutWidth = width;
    this.layoutHeight = height;
    this.startSpeed = Math.sqrt(width * width + height * height) / 2.0;
    double endSpeed = k / 10.0;
    this.base = Math.pow(endSpeed / startSpeed, 1.0 / (maxIterations - 1));
  }

  /**
   * Calculate the desired speed for a given iteration
   *
   * @param iteration Iteration to calculate speed for
   * @return Desired speed
   */
  public double speedForIteration(int iteration) {
    // cache last result to avoid costly pow
    if (iteration != lastIteration) {
      lastTemperature = startSpeed * Math.pow(base, iteration);
      lastIteration = iteration;
    }
    return lastTemperature;
  }

  /**
   * Apply force to position. Honoring speed limit anmd layouting-area.
   * MODIFIES given vectors!
   *
   * @param position   The position to modify
   * @param movement   The desired movement
   * @param speedLimit The speed limit for the movement
   */
  public void applyForce(Vector position, Vector movement, double speedLimit) {
    position.mAdd(movement.clamped(speedLimit));
    position.mConfined(0, layoutWidth - 1, 0, layoutHeight - 1);
  }

  @Override
  public LVertex join(LVertex first, Force second) throws Exception {

    double speedLimit = speedForIteration(getIterationRuntimeContext().getSuperstepNumber());
    Vector movement = second.getValue();
    Vector position = first.getPosition();
    applyForce(position, movement, speedLimit);
    first.setForce(movement);
    first.setPosition(position);
    return first;
  }

}
