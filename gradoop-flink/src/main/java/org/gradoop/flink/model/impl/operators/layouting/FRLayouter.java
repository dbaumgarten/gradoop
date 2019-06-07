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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
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
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Layouts a graph using the Fruchtermann-Reingold algorithm
 */
public class FRLayouter extends LayoutingAlgorithm {
  /**
   * Main-parameter of the FR-Algorithm. Optimum distance between connected vertices.
   */
  protected double k;
  /**
   * Number of iterations to perform
   */
  protected int iterations;
  /**
   * Width of the layouting-space
   */
  protected int width;
  /**
   * Height of the layouting-space
   */
  protected int height;
  /**
   * Maximum distance for computing repulsion-forces between vertices
   */
  protected int maxRepulsionDistance;

  /**
   * Create new Instance of FRLayouter
   *
   * @param k          Optimal distance between connected vertices. Optimal k can be computed
   *                   with calculateK()
   * @param iterations Number of iterations to perform of the algorithm
   * @param width      Width of the layouting space
   * @param height     Height of the layouting space
   * @param maxRepulsionDistance   Maximum distance between two vertices before stopping to compute
   *                   repulsions between them
   */
  public FRLayouter(double k, int iterations, int width, int height, int maxRepulsionDistance) {
    this.k = k;
    this.width = width;
    this.height = height;
    this.iterations = iterations;
    this.maxRepulsionDistance = maxRepulsionDistance;
  }

  /**
   * Calculates the optimal distance between two nodes connected by an edge
   *
   * @param width  Width of the layouting-space
   * @param height Height of the layouting-space
   * @param count  Number of vertices in the graph (does not need to be 100% precise)
   * @return The calculated k for the given input values
   */
  public static double calculateK(int width, int height, int count) {
    return Math.sqrt((width * height) / (double) count);
  }


  @Override
  public LogicalGraph execute(LogicalGraph g) {

    RandomLayouter rl =
      new RandomLayouter(width / 10, width - (width / 10), height / 10, height - (height / 10));
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
      .with(new FRForceApplicator(width, height, k, iterations));
  }


  /**
   * Calculates the repusive forces between the given vertices.
   *
   * @param vertices A dataset of vertices
   * @return Dataset of applied forces. May (and will) contain multiple forces for each vertex.
   */
  protected DataSet<Force> repulsionForces(DataSet<LVertex> vertices) {
    vertices = vertices.map(new FRCellIdMapper(maxRepulsionDistance));

    KeySelector<LVertex, Integer> selfselector =
      new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF);
    FRRepulsionFunction repulsionFunction = new FRRepulsionFunction(k, maxRepulsionDistance);

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
      }).flatMap(new FRAttractionFunction(k));
  }

}
