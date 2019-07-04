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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.DefaultVertexCompareFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexCompareFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexFusor;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.GraphElement;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

/**
 * A special variant of the FRLayouter that combines similar vertices during the layouting,
 * creating a simplified version of the graph.
 * ATTENTION! Edge- and Vertex properties are NOT retained in the output-graph.
 */
public class FusingFRLayouter extends FRLayouter {

  /**
   * Name of the property that will contain the number of sub-vertices or sub-edges for a vertex or
   * edge
   */
  public static final String VERTEX_SIZE_PROPERTY = "SIZE";
  /**
   * Only vertices with a similarity of at least threshold are combined
   */
  protected double threshold;
  /**
   * Compare function to use. Null means use default.
   */
  protected VertexCompareFunction compareFunction = null;

  /**
   * Create new FusingFRLayouter
   *
   * @param iterations  Number of iterations to perform
   * @param vertexCount Number of vertices in the input-graph (used to compute default-values)
   * @param threshold   nly vertices with a similarity of at least threshold are combined. Lower
   *                    values will lead to a more simplified output-graph. Valid values are >= 0
   *                    and <= 1
   */
  public FusingFRLayouter(int iterations, int vertexCount, double threshold) {
    super(iterations, vertexCount);
    this.threshold = threshold;
  }

  /**
   * Sets optional value compareFunction. If no custom function is used
   * DefaultVertexCompareFunction will be used.
   *
   * @param compareFunction the new value
   * @return this (for method-chaining)
   */
  public FusingFRLayouter compareFunction(VertexCompareFunction compareFunction) {
    this.compareFunction = compareFunction;
    return this;
  }

  /**
   * Gets compareFunction
   *
   * @return value of compareFunction
   */
  public VertexCompareFunction getCompareFunction() {
    return (compareFunction != null) ? compareFunction : new DefaultVertexCompareFunction(getK());
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {

    RandomLayouter rl =
      new RandomLayouter(getWidth() / 10, getWidth() - (getWidth() / 10), getHeight() / 10,
        getHeight() - (getHeight() / 10));
    g = rl.execute(g);

    DataSet<Vertex> gradoopVertices = g.getVertices();
    DataSet<Edge> gradoopEdges = g.getEdges();

    // Flink can only iterate over a single dataset. Therefore vertices and edges have to be
    // temporarily combined into a single dataset
    DataSet<GraphElement> tmpvertices = gradoopVertices.map((v) -> new LVertex(v));
    DataSet<GraphElement> tmpedges = gradoopEdges.map((e) -> new LEdge(e));
    DataSet<GraphElement> graphElements = tmpvertices.union(tmpedges);

    IterativeDataSet<GraphElement> loop = graphElements.iterate(iterations);

    // split the combined dataset to work with the edges and vertices
    DataSet<LVertex> vertices = loop.filter(e -> e instanceof LVertex).map(e -> (LVertex) e);
    DataSet<LEdge> edges = loop.filter(e -> e instanceof LEdge).map(e -> (LEdge) e);

    //Perform the layouting as usual
    DataSet<Force> repulsions = repulsionForces(vertices);
    DataSet<Force> attractions = attractionForces(vertices, edges);

    DataSet<Force> forces =
      repulsions.union(attractions).groupBy(Force.ID).reduce((first, second) -> {
        first.setValue(first.getValue().add(second.getValue()));
        return first;
      });

    vertices = applyForces(vertices, forces, iterations);

    // Use the VertexFusor to create a simplified version of the graph
    VertexFusor vf = new VertexFusor(getCompareFunction(), threshold);
    Tuple2<DataSet<LVertex>, DataSet<LEdge>> fusionResult = vf.execute(vertices, edges);
    vertices = fusionResult.f0;
    edges = fusionResult.f1;

    // again, combine vertices and edges into a single dataset to perform iterations
    graphElements = vertices.map(x -> (GraphElement) x).union(edges.map(x -> (GraphElement) x));
    graphElements = loop.closeWith(graphElements);

    // again, split the combined dataset  (after all iterations have been completed)
    DataSet<LVertex> finalVertices =
      graphElements.filter(e -> e instanceof LVertex).map(e -> (LVertex) e);
    DataSet<LEdge> finalEdges = graphElements.filter(e -> e instanceof LEdge).map(e -> (LEdge) e);

    // Create Gradoop vertices and edges from the internal representation.
    // TODO: using a join with the original graph for this is a dirty workaround. Do it properly.
    gradoopVertices = finalVertices.join(gradoopVertices).where(LVertex.ID).equalTo("id")
      .with(new JoinFunction<LVertex, Vertex, Vertex>() {
        @Override
        public Vertex join(LVertex lVertex, Vertex vertex) throws Exception {
          lVertex.getPosition().setVertexPosition(vertex);
          vertex.setProperty(VERTEX_SIZE_PROPERTY, lVertex.getCount());
          return vertex;
        }
      });

    gradoopEdges = finalEdges.join(gradoopEdges).where(LEdge.ID).equalTo("id")
      .with(new JoinFunction<LEdge, Edge, Edge>() {
        @Override
        public Edge join(LEdge lEdge, Edge edge) {
          edge.setSourceId(lEdge.getSourceId());
          edge.setTargetId(lEdge.getTargetId());
          edge.setProperty(VERTEX_SIZE_PROPERTY, lEdge.getCount());
          return edge;
        }
      });

    return g.getFactory().fromDataSets(gradoopVertices, gradoopEdges);
  }

}
