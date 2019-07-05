package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.java.DataSet;

public class LGraph {
  private DataSet<LVertex> vertices;
  private DataSet<LEdge> edges;

  public LGraph(DataSet<LVertex> vertices, DataSet<LEdge> edges) {
    this.vertices = vertices;
    this.edges = edges;
  }

  public LGraph (DataSet<GraphElement> g){
    vertices = g.filter(e -> e instanceof LVertex).map(e -> (LVertex) e);
    edges = g.filter(e -> e instanceof LEdge).map(e -> (LEdge) e);
  }

  public DataSet<GraphElement> getGraphElements(){
    return  vertices.map(x -> (GraphElement) x).union(edges.map(x -> (GraphElement) x));
  }

  /**
   * Gets vertices
   *
   * @return value of vertices
   */
  public DataSet<LVertex> getVertices() {
    return vertices;
  }

  /**
   * Sets vertices
   *
   * @param vertices the new value
   */
  public void setVertices(DataSet<LVertex> vertices) {
    this.vertices = vertices;
  }

  /**
   * Gets edges
   *
   * @return value of edges
   */
  public DataSet<LEdge> getEdges() {
    return edges;
  }

  /**
   * Sets edges
   *
   * @param edges the new value
   */
  public void setEdges(DataSet<LEdge> edges) {
    this.edges = edges;
  }
}
