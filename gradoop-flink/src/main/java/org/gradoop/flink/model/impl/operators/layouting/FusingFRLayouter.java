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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.DefaultVertexCompareFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexCompareFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexFusor;
import org.gradoop.flink.model.impl.operators.layouting.util.GraphElement;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.ArrayList;
import java.util.List;

/**
 * A special variant of the FRLayouter that combines similar vertices during the layouting,
 * creating a simplified version of the graph.
 */
public class FusingFRLayouter extends FRLayouter {

  public enum OutputFormat {
    /** Output the simplified graph. The output-graph will loose all information except for the
     * GradoopIds. Vertices/Edges will have "SUBELEMENTS"-Property listing all elements that were
     * combined into the super-element. Edges and Vertices will have a "SIZE"-Property containing
     * the number of sub-elements contained in this super-element.
     */
    SIMPLIFIED,
    /**
     * Grouped vertices will be resolved into the original vertices and will be placed randomly
     * close to another.
     */
    EXTRACTED,
    /**
     * Like EXTRACTED, but vertices of a supernode will be placed at exactly the same position.
     */
    RAWEXTRACTED
  }

  /**
   * Name of the property that will contain the number of sub-vertices or sub-edges for a vertex or
   * edge
   */
  public static final String VERTEX_SIZE_PROPERTY = "SIZE";
  /**
   * The name of the property where the ids of the sub-vertices (or sub-edges) of a
   * supervertex/superedge
   * are stored.
   */
  public static final String SUB_ELEMENTS_PROPERTY = "SUBELEMENTS";
  /**
   * Only vertices with a similarity of at least threshold are combined
   */
  protected double threshold;
  /**
   * Compare function to use. Null means use default.
   */
  protected VertexCompareFunction compareFunction = null;
  /**
   * The output format chosen by the user
   */
  protected OutputFormat outputFormat;

  /**
   * Create new FusingFRLayouter
   *
   * @param iterations  Number of iterations to perform
   * @param vertexCount Number of vertices in the input-graph (used to compute default-values)
   * @param threshold   nly vertices with a similarity of at least threshold are combined. Lower
   *                    values will lead to a more simplified output-graph. Valid values are >= 0
   *                    and <= 1
   * @param of Chosen OutputFormat. See {@link OutputFormat}
   */
  public FusingFRLayouter(int iterations, int vertexCount, double threshold, OutputFormat of) {
    super(iterations, vertexCount);
    this.threshold = threshold;
    this.outputFormat = of;
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
    // temporarily combined into a single dataset.
    // Also the Grapdoop datatypes are converted to internal datatypes
    DataSet<GraphElement> tmpvertices = gradoopVertices.map((v) -> new LVertex(v));
    DataSet<GraphElement> tmpedges = gradoopEdges.map((e) -> new LEdge(e));
    DataSet<GraphElement> graphElements = tmpvertices.union(tmpedges);

    IterativeDataSet<GraphElement> loop = graphElements.iterate(iterations);

    // split the combined dataset to work with the edges and vertices
    LGraph graph = new LGraph(loop);

    // perform the layouting
    layout(graph);

    // Use the VertexFusor to create a simplified version of the graph
    VertexFusor vf = new VertexFusor(getCompareFunction(), threshold);
    graph = vf.execute(graph);

    // again, combine vertices and edges into a single dataset to perform iterations
    graphElements = graph.getGraphElements();
    graphElements = loop.closeWith(graphElements);

    // again, split the combined dataset  (after all iterations have been completed)
    graph = new LGraph(graphElements);


    switch (outputFormat){
    case SIMPLIFIED:
      return buildSimplifiedGraph(g,graph);
    case EXTRACTED:
      return buildExtractedGraph(g,graph,2*getK());
    case RAWEXTRACTED:
      return buildExtractedGraph(g,graph,0);
    }

    // This should never happen
    return null;
  }

  /**
   * Simply translate the internal representations into GRadoop-types
   * @param input Original input graph
   * @param layouted Result of the layouting
   * @return The layouted graph in the Gradoop-format
   */
  protected LogicalGraph buildSimplifiedGraph(LogicalGraph input, LGraph layouted){
    DataSet<Vertex> vertices = layouted.getVertices().map((lv)->{
      Vertex v = new Vertex(lv.getId(),"vertex",new Properties(),null);
      lv.getPosition().setVertexPosition(v);
      v.setProperty(VERTEX_SIZE_PROPERTY, lv.getCount());
      v.setProperty(SUB_ELEMENTS_PROPERTY,getSubelementListValue(lv.getSubVertices()));
      return v;
    });

    DataSet<Edge> edges = layouted.getEdges().map((le)->{
      Edge e = new Edge(le.getId(),"edge",le.getSourceId(),le.getTargetId(),new Properties(),null);
      e.setProperty(VERTEX_SIZE_PROPERTY, le.getCount());
      e.setProperty(SUB_ELEMENTS_PROPERTY,getSubelementListValue(le.getSubEdges()));
      return e;
    });
    return input.getFactory().fromDataSets(vertices,edges);
  }

  /**
   * Extract all subverties/subedges from the super-vertices/super-edges and place them at the
   * location of the super-vertex (and add some random jitter to the positions)
   * @param input Original input graph
   * @param layouted Result of the layouting
   * @param jitter Maximum distance between super-node position and node-position
   * @return The final graph, containing all vertices and edges from the original graph.
   */
  protected LogicalGraph buildExtractedGraph(LogicalGraph input, LGraph layouted, final double jitter){
    DataSet<Vertex> vertices =
      layouted.getVertices().flatMap((FlatMapFunction<LVertex, LVertex>) (superv,collector)->{
      for (GradoopId id: superv.getSubVertices()){
        LVertex v = new LVertex();
        v.setId(id);
        v.setPosition(jitterPosition(superv.getPosition(),jitter));
        collector.collect(v);
      }
      superv.setSubVertices(null);
      collector.collect(superv);
    }).returns(new TypeHint<LVertex>() {}).join(input.getVertices()).where(LVertex.ID).equalTo(
      "id").with((lv,v)->{
      lv.getPosition().setVertexPosition(v);
      return v;
    });
    return input.getFactory().fromDataSets(vertices,input.getEdges());
  }

  /**
   * Add random jitter to position
   * @param center Position
   * @param jitter Maximum distance
   * @return Randomly modified position
   */
  protected static Vector jitterPosition(Vector center, double jitter){
    Vector offset = new Vector();
    while (true){
      double x = (Math.random()*jitter)-(jitter/2.0);
      double y = (Math.random()*jitter)-(jitter/2.0);
      offset.set(x,y);
      if (offset.magnitude() <= jitter){
        break;
      }
    }
    return offset.mAdd(center);
  }

  /**
   * Helper function to convert the List of sub-elements into a List of PropertyValues
   * @param ids List of GradoopIds
   * @return A Property value of type List<PropertyValue<GradoopId>>
   */
  protected static PropertyValue getSubelementListValue(List<GradoopId> ids){
    List<PropertyValue> result = new ArrayList<>();
    for (GradoopId id : ids){
      result.add(PropertyValue.create(id));
    }
    return PropertyValue.create(result);
  }

}
