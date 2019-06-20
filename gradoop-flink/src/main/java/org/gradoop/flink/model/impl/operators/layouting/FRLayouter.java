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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRAttractionFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdMapper;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdSelector;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRForceApplicator;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

/**
 * Layouts a graph using the Fruchtermann-Reingold algorithm
 */
public class FRLayouter extends LayoutingAlgorithm {

  protected static final double DEFAULT_K = 100;

  /**
   * Number of iterations to perform
   */
  protected int iterations;
  /**
   * User supplied k. Main-parameter of the FR-Algorithm. Optimum distance between connected
   * vertices.
   */
  protected double custom_k = 0;
  /**
   * User supplied width of the layouting-space
   */
  protected int custom_width = 0;
  /**
   * User supplied height of the layouting-space
   */
  protected int custom_height = 0;
  /**
   * User supplied maximum distance for computing repulsion-forces between vertices
   */
  protected int custom_maxRepulsionDistance = 0;
  /** (Estimated) number of vertices in the graph. Needed to calculate default
   *  parameters
   *
   */
  protected int numberOfVertices;


  /** Create new Instance of FRLayouter.
   *
   * @param iterations Number of iterations to perform
   * @param vertexCount (Estimated) number of vertices in the graph. Needed to calculate default
   *                    parammeters
   */
  public FRLayouter(int iterations, int vertexCount) {
    this.iterations = iterations;
    this.numberOfVertices = vertexCount;
  }


  /** Override default k-parameter of the FR-Algorithm
   * Default: 100
   * @param k new k
   * @return this (for method-chaining)
   */
  public FRLayouter k(double k){
    this.custom_k = k;
    return this;
  }

  /** Override default layout-space size
   * Default:  width = height = Math.sqrt(Math.pow(k, 2) * numberOfVertices) * 0.5
   * @param width new width
   * @param height new height
   * @return this (for method-chaining)
   */
  public FRLayouter area(int  width, int height){
    this.custom_width = width;
    this.custom_height = height;
    return this;
  }

  /** Override default maxRepulsionDistance of the FR-Algorithm. Vertices with larger distance
   * are ignored in repulsion-force calculation
   * Default-Value is relative to current k. If k is overriden, this is changed
   * accordingly automatically
   * Default: 2k
   * @param maxRepulsionDistance new value
   * @return this (for method-chaining)
   */
  public FRLayouter maxRepulsionDistance(int maxRepulsionDistance){
    this.custom_maxRepulsionDistance = maxRepulsionDistance;
    return this;
  }

  /**
   * Gets k
   *
   * @return value of k
   */
  public double getK() {
    return (custom_k!=0)?custom_k:DEFAULT_K;
  }


  @Override
  public int getWidth() {
    return (custom_width!=0)?custom_width:(int)(Math.sqrt(Math.pow(DEFAULT_K, 2) * numberOfVertices) * 0.5);
  }

  @Override
  public int getHeight() {
    return (custom_height!=0)?custom_height:(int)(Math.sqrt(Math.pow(DEFAULT_K, 2) * numberOfVertices) * 0.5);
  }

  /**
   * Gets maxRepulsionDistance
   *
   * @return value of maxRepulsionDistance
   */
  public int getMaxRepulsionDistance() {
    return (custom_maxRepulsionDistance!=0)?custom_maxRepulsionDistance:(int)(2*getK());
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {

    RandomLayouter rl =
      new RandomLayouter(getWidth() / 10, getWidth() - (getWidth() / 10), getHeight() / 10,
        getHeight() - (getHeight() / 10));
    g = rl.execute(g);

    DataSet<Vertex> gradoopVertices = g.getVertices();
    DataSet<Edge> gradoopEdges = g.getEdges();

    DataSet<LVertex> vertices = gradoopVertices.map((v)->new LVertex(v));
    DataSet<LEdge> edges = gradoopEdges.map((e)->new LEdge(e));

    IterativeDataSet<LVertex> loop = vertices.iterate(iterations);

    DataSet<Force> repulsions = repulsionForces(loop);

    DataSet<Force> attractions = attractionForces(loop, edges);

    DataSet<Force> forces =
      repulsions.union(attractions).groupBy(Force.ID).reduce((first,second)->{
        first.setValue(first.getValue().add(second.getValue()));
        return first;
        });

    DataSet<LVertex> moved = applyForces(loop, forces, iterations);

    vertices = loop.closeWith(moved);

    gradoopVertices =
      vertices.join(gradoopVertices).where(LVertex.ID).equalTo("id").with(new JoinFunction<LVertex,
        Vertex, Vertex>() {
      @Override
      public Vertex join(LVertex lVertex, Vertex vertex) throws Exception {
        lVertex.getPosition().setVertexPosition(vertex);
        return vertex;
      }
    });

    return g.getFactory().fromDataSets(gradoopVertices, gradoopEdges);
  }

  /**
   * Applies the given forces to the given vertices.
   *
   * @param vertices   Vertices to move
   * @param forces     Forces to apply. At most one per vertex. The id indicates which vertex
   *                   the force should be applied to
   * @param iterations Number of iterations that are/will be performed (NOT the number of the
   *                   current Iteration). Is to compute the simulated annealing shedule.
   * @return The input vertices with x and y coordinated chaned according to the given force and
   * current iteration number.
   */
  protected DataSet<LVertex> applyForces(DataSet<LVertex> vertices,
    DataSet<Force> forces, int iterations) {
    return vertices.join(forces).where(LVertex.ID).equalTo(Force.ID)
      .with(new FRForceApplicator(getWidth(), getHeight(), getK(), iterations));
  }


  /**
   * Calculates the repusive forces between the given vertices.
   *
   * @param vertices A dataset of vertices
   * @return Dataset of applied forces. May (and will) contain multiple forces for each vertex.
   */
  protected DataSet<Force> repulsionForces(DataSet<LVertex> vertices) {
    vertices = vertices.map(new FRCellIdMapper(getMaxRepulsionDistance()));

    KeySelector<LVertex, Integer> selfselector =
      new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF);
    FRRepulsionFunction repulsionFunction = new FRRepulsionFunction(getK(), getMaxRepulsionDistance());

    DataSet<Force> self = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF))
      .equalTo(selfselector).with((JoinFunction<LVertex,LVertex,Force>)repulsionFunction);

    DataSet<Force> up = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UP))
      .equalTo(selfselector).with((FlatJoinFunction<LVertex,LVertex,Force>)repulsionFunction);

    DataSet<Force> left = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.LEFT))
      .equalTo(selfselector).with((FlatJoinFunction<LVertex,LVertex,Force>)repulsionFunction);

    DataSet<Force> uright = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UPRIGHT))
      .equalTo(selfselector).with((FlatJoinFunction<LVertex,LVertex,Force>)repulsionFunction);

    DataSet<Force> uleft = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UPLEFT))
      .equalTo(selfselector).with((FlatJoinFunction<LVertex,LVertex,Force>)repulsionFunction);


    return self.union(up).union(left).union(uright).union(uleft);
  }

  /**
   * Compute the attractive-forces between all vertices connected by edges.
   *
   * @param vertices The vertices
   * @param edges    The edges between vertices
   * @return A mapping from VertexId to x and y forces
   */
  protected DataSet<Force> attractionForces(DataSet<LVertex> vertices,
    DataSet<LEdge> edges) {
    return edges.join(vertices).where(LEdge.SOURCE_ID).equalTo(LVertex.ID).join(vertices).where(
      "f0."+LEdge.TARGET_ID)
      .equalTo(LVertex.ID).with((first,second)->new Tuple2<LVertex,LVertex>(first.f1,second)).returns(new TypeHint<Tuple2<LVertex, LVertex>>() {
      }).flatMap(new FRAttractionFunction(getK()));
  }


  @Override
  public String toString() {
    return "FRLayouter{" + "iterations=" + iterations + ", k=" + getK() + ", width=" + getWidth() +
      ", height=" + getHeight() + ", maxRepulsionDistance=" + getMaxRepulsionDistance() +
      ", numberOfVertices=" + numberOfVertices + '}';
  }
}
