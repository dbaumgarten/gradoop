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
package org.gradoop.flink.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;

/**
 * Operator to determine if two graph are equal according to given string
 * representations of graph heads, vertices and edges.
 */
public class GraphEquality
  implements BinaryBaseGraphToValueOperator<LogicalGraph, DataSet<Boolean>> {

  /**
   * collection equality operator, wrapped by graph equality
   * (graph are considered to be a 1-graph collection)
   */
  private final CollectionEquality collectionEquality;
  /**
   * sets mode for directed or undirected graphs
   */
  private final boolean directed;

  /**
   * constructor to set string representations
   * @param graphHeadToString string representation of graph heads
   * @param vertexToString string representation of vertices
   * @param edgeToString string representation of edges
   * @param directed sets mode for directed or undirected graphs
   */
  public GraphEquality(GraphHeadToString<EPGMGraphHead> graphHeadToString,
    VertexToString<EPGMVertex> vertexToString, EdgeToString<EPGMEdge> edgeToString,
    boolean directed) {
    this.directed = directed;

    this.collectionEquality =
      new CollectionEquality(graphHeadToString, vertexToString, edgeToString, this.directed);
  }

  @Override
  public DataSet<Boolean> execute(LogicalGraph firstGraph, LogicalGraph secondGraph) {
    BaseGraphCollectionFactory<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>
      collectionFactory = firstGraph.getCollectionFactory();
    return collectionEquality
      .execute(collectionFactory.fromGraph(firstGraph), collectionFactory.fromGraph(secondGraph));
  }
}
