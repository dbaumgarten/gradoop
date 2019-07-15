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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.gelly.GradoopGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRAttractionFunction;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRForceApplicator;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Implementation of the GiLa-Layouting-Algorithm.
 * Good for sparsely-connected graphs. Really slow for dense graphs.
 */
public class GiLaLayouter extends
  GradoopGellyAlgorithm<GiLaLayouter.VertexValue, NullValue> implements LayoutingAlgorithm {

  /**
   * Default optimum distance.
   */
  protected static final double DEFAULT_OPT_DISTANCE = 100;
  /**
   * The MessageFunction to use
   */
  protected MsgFunc msgFunc;
  /**
   * Number of total iterations to perform
   */
  protected int iterations;
  /**
   * Width of the layouting-area
   */
  protected int width;
  /**
   * Height of the layouting-area
   */
  protected int height;
  /**
   * Only vertices in the kNeigboorhood of a vertex are used for repulsion calculation
   */
  protected int kNeighborhood;
  /**
   * This is the constance K from the FRLayouter. Renamed so it is not confused with
   * kNeighborhood
   */
  protected double optimumDistance;
  /**
   * (Approximate) Number of vertices in the graph. Used to compute default-values.
   */
  protected int numberOfVertices;

  /**
   * Construct a new GiLaLayouter-Instance
   *
   * @param iterations    Number of iterations to perform
   * @param vertexCount   (Approximate) Number of vertices in the graph. Used to compute
   *                      default-values.
   * @param kNeighborhood Only vertices in the kNeigboorhood of a vertex are used for repulsion
   *                      calculation.
   */
  public GiLaLayouter(int iterations, int vertexCount, int kNeighborhood) {
    super(new VertexToGellyVertex<VertexValue>() {
      @Override
      public Vertex<GradoopId, VertexValue> map(org.gradoop.common.model.impl.pojo.Vertex vertex) {
        return new Vertex<>(vertex.getId(), new VertexValue(vertex));
      }
    }, new EdgeToGellyEdgeWithNullValue());
    this.iterations = iterations;
    this.kNeighborhood = kNeighborhood;
    this.numberOfVertices = vertexCount;
  }

  /**
   * Override default layout-space size
   * Default:  width = height = Math.sqrt(Math.pow(k, 2) * numberOfVertices) * 0.5
   *
   * @param width  new width
   * @param height new height
   * @return this (for method-chaining)
   */
  public GiLaLayouter area(int width, int height) {
    this.width = width;
    this.height = height;
    return this;
  }

  /**
   * Sets optional value optimumDistance
   *
   * @param optimumDistance the new value
   * @return this (for method-chaining)
   */
  public GiLaLayouter optimumDistance(double optimumDistance) {
    this.optimumDistance = optimumDistance;
    return this;
  }

  /**
   * Gets optimumDistance
   *
   * @return value of optimumDistance
   */
  public double getOptimumDistance() {
    return (optimumDistance != 0) ? optimumDistance : DEFAULT_OPT_DISTANCE;
  }

  @Override
  public int getWidth() {
    return (width != 0) ? width :
      (int) (Math.sqrt(Math.pow(DEFAULT_OPT_DISTANCE, 2) * numberOfVertices) * 0.5);
  }

  @Override
  public int getHeight() {
    return (height != 0) ? height :
      (int) (Math.sqrt(Math.pow(DEFAULT_OPT_DISTANCE, 2) * numberOfVertices) * 0.5);
  }


  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    msgFunc = new MsgFunc(iterations, getWidth(), getHeight(), getOptimumDistance(), kNeighborhood);
    RandomLayouter rl =
      new RandomLayouter(getWidth() / 10, getWidth() - (getWidth() / 10), getHeight() / 10,
        getHeight() - (getHeight() / 10));
    graph = rl.execute(graph);

    // GiLa needs an undirected graph. Transform the undirected into a directed Graph by copying
    // and reversing all edges.
    DataSet<org.gradoop.common.model.impl.pojo.Edge> edges =
      graph.getEdges().flatMap((FlatMapFunction<org.gradoop.common.model.impl.pojo.Edge,
        org.gradoop.common.model.impl.pojo.Edge>)(e,
      collector)->{
      org.gradoop.common.model.impl.pojo.Edge edgeCopy =
        new org.gradoop.common.model.impl.pojo.Edge(GradoopId.get(),e.getLabel(),e.getTargetId(),e.getSourceId(),
          new Properties(),null);
      collector.collect(e);
      collector.collect(edgeCopy);
    }).returns(new TypeHint<org.gradoop.common.model.impl.pojo.Edge>() {});

    graph = graph.getConfig().getLogicalGraphFactory().fromDataSets(graph.getVertices(),
      edges);

    return super.execute(graph);
  }

  @Override
  public LogicalGraph executeInGelly(Graph<GradoopId, VertexValue, NullValue> graph) {
    DataSet<Vertex<GradoopId, VertexValue>> result =
      graph.runVertexCentricIteration(msgFunc, null, iterations * kNeighborhood + 1).getVertices();


    DataSet<org.gradoop.common.model.impl.pojo.Vertex> layoutedVertices =
      result.join(currentGraph.getVertices()).where(0).equalTo("id").with(
        new JoinFunction<Vertex<GradoopId, VertexValue>,
          org.gradoop.common.model.impl.pojo.Vertex, org.gradoop.common.model.impl.pojo.Vertex>() {
          @Override
          public org.gradoop.common.model.impl.pojo.Vertex join(
            Vertex<GradoopId, VertexValue> gellyVertex,
            org.gradoop.common.model.impl.pojo.Vertex vertex) throws Exception {
            gellyVertex.getValue().position.setVertexPosition(vertex);
            return vertex;
          }
        });

    return currentGraph.getConfig().getLogicalGraphFactory()
      .fromDataSets(layoutedVertices, currentGraph.getEdges());
  }


  /**
   * The ComputeFunction for the GiLa-Algorithm
   */
  protected static class MsgFunc extends
    ComputeFunction<GradoopId, VertexValue, NullValue, Message> {

    /**
     * Only vertices in the kNeigboorhood of a vertex are used for repulsion calculation
     */
    protected int kNeighborhood;

    /**
     * Repulsion-function to use
     */
    protected FRRepulsionFunction repulsion;
    /**
     * Attraction-function to use
     */
    protected FRAttractionFunction attraction;
    /**
     * Application-Function to use
     */
    protected FRForceApplicator applicator;

    /**
     * For object-reuse
     */
    protected LVertex lvertex1 = new LVertex();

    /**
     * For object-reuse
     */
    protected LVertex lVertex2 = new LVertex();

    /**
     * For object-reuse
     */
    protected Tuple3<LVertex, LVertex, Integer> vertexTuple = new Tuple3<>();

    /**
     * Create new MsgFunc
     *
     * @param iterations      total number of iterations to perform
     * @param width           width of the layouting-area
     * @param height          height of the layouting-area
     * @param optimumDistance k of FRLayouter
     * @param kNeighborhood   kNeighborhood for repulsion-calculations
     */
    public MsgFunc(int iterations, int width, int height, double optimumDistance,
      int kNeighborhood) {
      this.kNeighborhood = kNeighborhood;

      this.repulsion = new FRRepulsionFunction(optimumDistance);
      this.attraction = new FRAttractionFunction(optimumDistance);
      this.applicator = new FRForceApplicator(width, height, optimumDistance, iterations);
    }

    @Override
    public void compute(Vertex<GradoopId, VertexValue> vertex,
      MessageIterator<Message> messageIterator) {

      // substact one to get 0-based numbers
      int iteration = getSuperstepNumber() - 1;

      VertexValue value = vertex.getValue();

      List<Message> messagesToSend = new LinkedList<>();

      int receivedMessages = 0;
      for (Message msg : messageIterator) {
        if (!value.messages.contains(msg.f0)) {
          value.messages.add(msg.getSender());

          //Attraction from direct neighbors
          if (msg.f2 == kNeighborhood) {
            value.forces = value.forces.add(
              attractionForce(vertex.getId(), value.position, msg.getSender(), msg.getPosition()));
          }

          //Repulsion from all
          value.forces = value.forces.add(
            repulsionForce(vertex.getId(), value.position, msg.getSender(), msg.getPosition()));
          if (msg.getTTL() > 1) {
            msg.setTTL(msg.getTTL() - 1);
            messagesToSend.add(msg);
          }
        }
        receivedMessages++;
      }

      if (iteration % kNeighborhood == 0 && iteration != 0) {
        applicator.apply(value.position, value.forces,
          applicator.speedForIteration(iteration / kNeighborhood));
        value.messages.clear();
        value.forces.reset();
      }

      if (iteration % kNeighborhood == 0 || iteration != 0 || receivedMessages == 0) {
        messagesToSend
          .add(new Message(vertex.getId(), value.position, kNeighborhood, vertex.getId()));
      }

      for (Edge<GradoopId, NullValue> e : getEdges()) {
        if (!e.getSource().equals(vertex.getId())){
          System.out.println("HOSSA!");
        }
        for (Message msg : messagesToSend) {
          // do not send message back to the vertex we received it from
          if (!msg.getLastHop().equals(e.getTarget())) {
            Message toSend = msg.copy();
            toSend.setLastHop(vertex.getId());
            sendMessageTo(e.getTarget(), toSend);
          }
        }
      }

      setNewVertexValue(value);
    }

    /**
     * Get attraction forces between two vertices
     *
     * @param id1  id of vertex 1
     * @param pos1 position of vertex 1
     * @param id2  id ov vertex 2
     * @param pos2 position of vertex 2
     * @return The calculated force-vector for vertex 1
     */
    protected Vector attractionForce(GradoopId id1, Vector pos1, GradoopId id2, Vector pos2) {
      lvertex1.setId(id1);
      lvertex1.setPosition(pos1.copy());
      lVertex2.setId(id2);
      lVertex2.setPosition(pos2.copy());
      vertexTuple.f0 = lvertex1;
      vertexTuple.f1 = lVertex2;
      vertexTuple.f2 = 1;
      return attraction.map(vertexTuple).getValue().copy();
    }

    /**
     * Get repulsion forces between two vertices
     *
     * @param id1  id of vertex 1
     * @param pos1 position of vertex 1
     * @param id2  id ov vertex 2
     * @param pos2 position of vertex 2
     * @return The calculated force-vector for vertex 1
     */
    protected Vector repulsionForce(GradoopId id1, Vector pos1, GradoopId id2, Vector pos2) {
      lvertex1.setId(id1);
      lvertex1.setPosition(pos1.copy());
      lVertex2.setId(id2);
      lVertex2.setPosition(pos2.copy());
      return repulsion.join(lvertex1, lVertex2).getValue().copy();
    }
  }

  /**
   * Represents a message transmitted between vertices. Just wraps Tuple4 for better readability.
   */
  protected static class Message extends Tuple4<GradoopId, Vector, Integer, GradoopId> {

    /**
     * Construct a new Message
     * @param sender Initial Sender of the message
     * @param position Position of the sender
     * @param ttl TimeToLive of this message
     * @param lastHop Id of the last vertex that retransmitted this message
     */
    public Message(GradoopId sender, Vector position, Integer ttl, GradoopId lastHop) {
      super(sender, position, ttl, lastHop);
    }

    /**
     * Default constructor to conform with Pojo-rules
     */
    public Message() {
      super();
    }

    /**
     * Get the initial sender of the message
     * @return The senders if
     */
    public GradoopId getSender() {
      return f0;
    }

    /**
     * Get the position of the sender
     * @return The position
     */
    public Vector getPosition() {
      return f1;
    }

    /**
     * Get the ttl of this message
     * @return the ttl
     */
    public Integer getTTL() {
      return f2;
    }

    /**
     * Get the last vertex send retransmitted this message
     * @return The id of sais vertex
     */
    public GradoopId getLastHop() {
      return f3;
    }

    /**
     * Set the sender id
     * @param id New id
     */
    public void setSender(GradoopId id) {
      f0 = id;
    }

    /**
     * Set the position
     * @param position New position
     */
    public void setPosition(Vector position) {
      f1 = position;
    }

    /**
     * Set the TTL for this message
     * @param ttl New TTL
     */
    public void setTTL(Integer ttl) {
      f2 = ttl;
    }

    /**
     * Set the last hop id of this message
     * @param hop New id
     */
    public void setLastHop(GradoopId hop) {
      f3 = hop;
    }

    /**
     * Create a copy of this message.
     * @return A shallow copy of this message
     */
    public Message copy() {
      return new Message(f0, f1, f2, f3);
    }
  }

  /**
   * Represents the stored values for each vertex.
   */
  protected static class VertexValue {
    /**
     * Current position of the vertex
     */
    protected Vector position;
    /**
     * Current aggregated forces acting on this vertex. To be applied at the end of the round
     */
    protected Vector forces;
    /**
     * IDs of vertice whos broadcasts were received this round
     */
    protected HashSet<GradoopId> messages;

    /**
     * Construct ne vertex-value
     *
     * @param v The gradoop-vertex to extract the position from
     */
    public VertexValue(org.gradoop.common.model.impl.pojo.Vertex v) {
      position = Vector.fromVertexPosition(v);
      forces = new Vector(0, 0);
      messages = new HashSet<>();
    }
  }

  @Override
  public String toString() {
    return "GiLaLayouter{" + "iterations=" + iterations + ", width=" + getWidth() + ", height=" +
      getHeight() + ", kNeighborhood=" + kNeighborhood + ", optimumDistance=" +
      getOptimumDistance() + ", numberOfVertices=" + numberOfVertices + '}';
  }
}
