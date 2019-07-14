package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.GraphElement;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.List;

public class CentroidFRLayouter extends FRLayouter {

  protected static final double INITIAL_SAMPLING_RATE = 0.1d;

  protected static final double MIN_MASS_FACTOR = 0.0025d;

  protected static final double MAX_MASS_FACTOR = 0.05d;

  protected static final String CENTROID_BROADCAST_NAME = "centroids";

  protected static final String CENTER_BROADCAST_NAME = "center";

  protected DataSet<Centroid> centroids;

  protected DataSet<Vector> center;

  public CentroidFRLayouter(int iterations, int vertexCount) {
    super(iterations, vertexCount);
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {

    g = createInitialLayout(g);

    DataSet<Vertex> gradoopVertices = g.getVertices();
    DataSet<Edge> gradoopEdges = g.getEdges();

    DataSet<LVertex> vertices = gradoopVertices.map((v) -> new LVertex(v));
    DataSet<LEdge> edges = gradoopEdges.map((e) -> new LEdge(e));

    centroids = chooseInitialCentroids(vertices);

    DataSet<GraphElement> graphElements = vertices.map(x->x);
    graphElements = graphElements.union(centroids.map(x->x));

    IterativeDataSet<GraphElement> loop = graphElements.iterate(iterations);
    vertices = loop.filter(x->x instanceof LVertex).map(x->(LVertex) x);
    centroids = loop.filter(x->x instanceof Centroid).map(x->(Centroid) x);

    centroids = updateCentroids(centroids,vertices);
    center = updateLayoutCenter(vertices);

    LGraph graph = new LGraph(vertices,edges);
    layout(graph);

    graphElements = graph.getVertices().map(x->x);
    graphElements = graphElements.union(centroids.map(x->x));

    graphElements = loop.closeWith(graphElements);

    vertices = graphElements.filter(x->x instanceof LVertex).map(x->(LVertex)x);

    gradoopVertices = vertices.join(gradoopVertices).where(LVertex.ID).equalTo("id")
      .with(new JoinFunction<LVertex, Vertex, Vertex>() {
        @Override
        public Vertex join(LVertex lVertex, Vertex vertex) throws Exception {
          lVertex.getPosition().setVertexPosition(vertex);
          return vertex;
        }
      });

    return g.getFactory().fromDataSets(gradoopVertices, gradoopEdges);
  }

  @Override
  protected DataSet<Force> repulsionForces(DataSet<LVertex> vertices) {
    FRRepulsionFunction rf = new FRRepulsionFunction(getK());
    return vertices.flatMap(new RepulsionForceCalculator(rf)).withBroadcastSet(centroids,
      CENTROID_BROADCAST_NAME).withBroadcastSet(center,CENTER_BROADCAST_NAME);
  }

  protected DataSet<Centroid> chooseInitialCentroids(DataSet<LVertex> vertices){
    return vertices.filter((v)->Math.random()<INITIAL_SAMPLING_RATE).map(v->new Centroid(v.getPosition(),0));
  }

  protected DataSet<Centroid> updateCentroids(DataSet<Centroid> centroids,
    DataSet<LVertex> vertices){

    CentroidUpdater upd = new CentroidUpdater(numberOfVertices);

    centroids = centroids.flatMap(upd);

    return vertices.map(upd).withBroadcastSet(centroids,CENTROID_BROADCAST_NAME).groupBy(Force.ID).reduceGroup(upd);
  }

  protected DataSet<Vector> updateLayoutCenter(DataSet<LVertex> vertices){
    return averageVector(vertices);
  }

  protected DataSet<Vector> averageVector(DataSet<LVertex> vertices){
    return vertices.reduceGroup(new GroupReduceFunction<LVertex, Vector>() {
      @Override
      public void reduce(Iterable<LVertex> iterable, Collector<Vector> collector) throws Exception {
        int count = 0;
        Vector sum = new Vector();
        for (LVertex v : iterable){
          count++;
          sum.mAdd(v.getPosition());
        }
        collector.collect(sum.mDiv(count));
      }
    });
  }

  protected static class RepulsionForceCalculator extends RichFlatMapFunction<LVertex,Force>{
    protected List<Centroid> centroids;
    protected List<Vector> center;

    protected LVertex centroidVertex = new LVertex();
    protected FRRepulsionFunction rf;

    public RepulsionForceCalculator(FRRepulsionFunction rf) {
      this.rf = rf;
    }

    public RepulsionForceCalculator(FRRepulsionFunction rf, List<Centroid> centroids, List<Vector> center) {
      this.rf = rf;
      this.centroids = centroids;
      this.center = center;
      centroidVertex.setId(GradoopId.get());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      centroids = getRuntimeContext().getBroadcastVariable(CENTROID_BROADCAST_NAME);
      center = getRuntimeContext().getBroadcastVariable(CENTER_BROADCAST_NAME);
    }


    @Override
    public void flatMap(LVertex vertex, Collector<Force> collector) {
      for (Centroid c : centroids){
        centroidVertex.setId(c.getId());
        centroidVertex.setPosition(c.getPosition().copy());
        Force f = rf.join(vertex,centroidVertex).copy();
        collector.collect(f);
      }
      centroidVertex.setPosition(center.get(0));
      Force f = rf.join(vertex,centroidVertex).copy();
      collector.collect(f);
    }
  }

  protected static class CentroidUpdater extends RichMapFunction<LVertex, Force>
    implements FlatMapFunction<Centroid, Centroid>,
    GroupReduceFunction<Force, Centroid>{

    protected int vertexCount;
    protected List<Centroid> centroids;

    public CentroidUpdater(int vertexCount) {
      this.vertexCount = vertexCount;
    }

    public CentroidUpdater(int vertexCount, List<Centroid> centroids) {
      this.vertexCount = vertexCount;
      this.centroids = centroids;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      if(getRuntimeContext().hasBroadcastVariable(CENTROID_BROADCAST_NAME)) {
        centroids = getRuntimeContext().getBroadcastVariable(CENTROID_BROADCAST_NAME);
      }
    }

    @Override
    public void flatMap(Centroid c, Collector<Centroid> collector) {
      if (c.getCount() == 0){
        collector.collect(c);
      }else if (c.getCount() < MIN_MASS_FACTOR * vertexCount){
        // do nothing
      }else if (c.getCount() > MAX_MASS_FACTOR * vertexCount){
        Centroid splitted = new Centroid(c.getPosition().add(new Vector(Math.random()*2-1,
          Math.random()*2-1)),
          c.getCount()/2);
        c.setCount(c.getCount()/2);
        collector.collect(c);
        collector.collect(splitted);
      }else{
        collector.collect(c);
      }
    }

    @Override
    public Force map(LVertex vertex)  {
      Force best = new Force();
      double bestDist = Double.MAX_VALUE;
      for (Centroid c : centroids){
        double dist = c.getPosition().distance(vertex.getPosition());
        if (dist < bestDist){
          best.set(c.getId(),vertex.getPosition());
          bestDist = dist;
        }
      }
      return best;
    }

    @Override
    public void reduce(Iterable<Force> iterable, Collector<Centroid> collector) {
      int count = 0;
      Vector posSum = new Vector();
      for (Force f : iterable){
        count ++;
        posSum.mAdd(f.getValue());
      }
      collector.collect(new Centroid(posSum.mDiv(count),count));
    }

  }


  protected static class Centroid implements GraphElement{
    protected GradoopId id;
    protected Vector position;
    protected int count;

    public Centroid(Vector position, int count) {
      id = GradoopId.get();
      this.position = position;
      this.count = count;
    }

    /**
     * Gets position
     *
     * @return value of position
     */
    public Vector getPosition() {
      return position;
    }

    /**
     * Sets position
     *
     * @param position the new value
     */
    public void setPosition(Vector position) {
      this.position = position;
    }

    /**
     * Gets count
     *
     * @return value of count
     */
    public int getCount() {
      return count;
    }

    /**
     * Sets count
     *
     * @param count the new value
     */
    public void setCount(int count) {
      this.count = count;
    }

    /**
     * Gets id
     *
     * @return value of id
     */
    public GradoopId getId() {
      return id;
    }

    /**
     * Sets id
     *
     * @param id the new value
     */
    public void setId(GradoopId id) {
      this.id = id;
    }
  }

  @Override
  public String toString() {
    return "Centroid" + super.toString();
  }
}
