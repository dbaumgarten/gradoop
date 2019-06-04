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
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Layouts a graph using the Fruchtermann-Reingold algorithm
 */
public class FRLayouter extends LayoutingAlgorithm {

  /**
   * Name of the property the cellid for a vertex is stored in
   */
  public static final String CELLID_PROPERTY = "cellid";

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

    DataSet<Vertex> vertices = g.getVertices();
    DataSet<Edge> edges = g.getEdges();

    IterativeDataSet<Vertex> loop = vertices.iterate(iterations);

    DataSet<Tuple2<GradoopId, Vector>> repulsions = repulsionForces(loop);

    DataSet<Tuple2<GradoopId, Vector>> attractions = attractionForces(loop, edges);

    DataSet<Tuple2<GradoopId, Vector>> forces =
      repulsions.union(attractions).groupBy(0).reduce((first,second)->{
        first.f1 = first.f1.add(second.f1);
        return first;
        });

    DataSet<Vertex> moved = applyForces(loop, forces, iterations);

    vertices = loop.closeWith(moved);

    return g.getFactory().fromDataSets(vertices, edges);
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
  protected DataSet<Vertex> applyForces(DataSet<Vertex> vertices,
    DataSet<Tuple2<GradoopId, Vector>> forces, int iterations) {
    return vertices.join(forces).where("id").equalTo(0)
      .with(new FRForceApplicator(width, height, k, iterations));
  }


  /**
   * Calculates the repusive forces between the given vertices.
   *
   * @param vertices A dataset of vertices
   * @return Dataset of applied forces. May (and will) contain multiple forces for each vertex.
   */
  protected DataSet<Tuple2<GradoopId, Vector>> repulsionForces(DataSet<Vertex> vertices) {
    vertices = vertices.map(new FRCellIdMapper(maxRepulsionDistance));

    KeySelector<Vertex, Integer> selfselector =
      new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF);
    FRRepulsionFunction repulsionFunction = new FRRepulsionFunction(k, maxRepulsionDistance);

    DataSet<Tuple2<GradoopId, Vector>> self = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.SELF))
      .equalTo(selfselector).with((JoinFunction)repulsionFunction);

    DataSet<Tuple2<GradoopId, Vector>> up = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UP))
      .equalTo(selfselector).with((FlatJoinFunction)repulsionFunction);

    DataSet<Tuple2<GradoopId, Vector>> left = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.LEFT))
      .equalTo(selfselector).with((FlatJoinFunction)repulsionFunction);

    DataSet<Tuple2<GradoopId, Vector>> uright = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UPRIGHT))
      .equalTo(selfselector).with((FlatJoinFunction)repulsionFunction);

    DataSet<Tuple2<GradoopId, Vector>> uleft = vertices.join(vertices)
      .where(new FRCellIdSelector(FRCellIdSelector.NeighborType.UPLEFT))
      .equalTo(selfselector).with((FlatJoinFunction)repulsionFunction);


    return self.union(up).union(left).union(uright).union(uleft);
  }

  /**
   * Compute the attractive-forces between all vertices connected by edges.
   *
   * @param vertices The vertices
   * @param edges    The edges between vertices
   * @return A mapping from VertexId to x and y forces
   */
  protected DataSet<Tuple2<GradoopId, Vector>> attractionForces(DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return edges.join(vertices).where("sourceId").equalTo("id").join(vertices).where("f0.targetId")
      .equalTo("id").with((first,second)->new Tuple2<Vertex,Vertex>(first.f1,second)).returns(new TypeHint<Tuple2<Vertex, Vertex>>() {
      }).flatMap(new FRAttractionFunction(k));
  }

}
