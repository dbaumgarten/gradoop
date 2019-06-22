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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.Random;

/**
 * LayoutingAlgorithm that positions all vertices randomly
 */
public class RandomLayouter extends LayoutingAlgorithm implements MapFunction<Vertex, Vertex> {

  /**
   * Minimum value for x coordinates
   */
  private int minX;
  /**
   * Maximum value for x coordinates
   */
  private int maxX;
  /**
   * Minimum value for y coordinates
   */
  private int minY;
  /**
   * Maximum value for y coordinates
   */
  private int maxY;
  /**
   * Rng to use for coordinate-generation
   */
  private Random rng;

  /**
   * Create a new RandomLayouter
   *
   * @param minX Minimum value of x-coordinate
   * @param maxX Maximum value of x-coordinate
   * @param minY Minimum value of y-coordinate
   * @param maxY Maximum value of y-coordinate
   */
  public RandomLayouter(int minX, int maxX, int minY, int maxY) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
  }


  @Override
  public LogicalGraph execute(LogicalGraph g) {
    if (rng == null) {
      rng = new Random();
    }
    DataSet<Vertex> placed = g.getVertices().map(this);
    return g.getFactory().fromDataSets(placed, g.getEdges());
  }


  //TODO having this public method just to deal with serializability of the map-class is ugly
  @Override
  public Vertex map(Vertex old) throws Exception {
    PropertyValue xcoord = PropertyValue.create(rng.nextInt(maxX - minX) + minX);
    PropertyValue ycoord = PropertyValue.create(rng.nextInt(maxY - minY) + minY);
    old.setProperty(X_COORDINATE_PROPERTY, xcoord);
    old.setProperty(Y_COORDINATE_PROPERTY, ycoord);
    return old;
  }

  @Override
  public int getWidth() {
    return maxX;
  }

  @Override
  public int getHeight() {
    return maxY;
  }
}
