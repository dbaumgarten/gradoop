package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.functions.VertexFusor;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.GraphElement;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;

public class FusingFRLayouter extends FRLayouter {

  public static final String VERTEX_SIZE_PROPERTY = "SIZE";

  protected double threshold;

  public FusingFRLayouter(int iterations, int vertexCount, double threshold) {
    super(iterations, vertexCount);
    this.threshold = threshold;
  }

  @Override
  public LogicalGraph execute(LogicalGraph g) {
    RandomLayouter rl =
      new RandomLayouter(getWidth() / 10, getWidth() - (getWidth() / 10), getHeight() / 10,
        getHeight() - (getHeight() / 10));
    g = rl.execute(g);

    DataSet<Vertex> gradoopVertices = g.getVertices();
    DataSet<Edge> gradoopEdges = g.getEdges();

    DataSet<GraphElement> tmpvertices = gradoopVertices.map((v) -> new LVertex(v));
    DataSet<GraphElement> tmpedges = gradoopEdges.map((e) -> new LEdge(e));
    DataSet<GraphElement> graphElements = tmpvertices.union(tmpedges);

    IterativeDataSet<GraphElement> loop = graphElements.iterate(iterations);

    DataSet<LVertex> vertices = loop.filter(e->e instanceof LVertex).map(e->(LVertex)e);
    DataSet<LEdge> edges = loop.filter(e->e instanceof LEdge).map(e->(LEdge) e);

    DataSet<Force> repulsions = repulsionForces(vertices);
    DataSet<Force> attractions = attractionForces(vertices, edges);

    DataSet<Force> forces =
      repulsions.union(attractions).groupBy(Force.ID).reduce((first, second) -> {
        first.setValue(first.getValue().add(second.getValue()));
        return first;
      });

    vertices = applyForces(vertices, forces, iterations);

    VertexFusor vf = new VertexFusor(new SimilarityCalculator(getK()),threshold);
    Tuple2<DataSet<LVertex>,DataSet<LEdge>> fusionResult = vf.execute(vertices,edges);
    vertices = fusionResult.f0;
    edges = fusionResult.f1;

    graphElements = vertices.map(x->(GraphElement)x).union(edges.map(x->(GraphElement) x));

    graphElements = loop.closeWith(graphElements);

    DataSet<LVertex> finalVertices = graphElements.filter(e->e instanceof LVertex).map(e->(LVertex)e);
    DataSet<LEdge> finalEdges = graphElements.filter(e->e instanceof LEdge).map(e->(LEdge) e);



    gradoopVertices = finalVertices.join(gradoopVertices).where(LVertex.ID).equalTo("id")
      .with(new JoinFunction<LVertex, Vertex, Vertex>() {
        @Override
        public Vertex join(LVertex lVertex, Vertex vertex) throws Exception {
          lVertex.getPosition().setVertexPosition(vertex);
          vertex.setProperty(VERTEX_SIZE_PROPERTY,lVertex.getCount());
          return vertex;
        }
      });

    gradoopEdges = finalEdges.join(gradoopEdges).where(LEdge.ID).equalTo("id").with(new JoinFunction<LEdge, Edge, Edge>() {
      @Override
      public Edge join(LEdge lEdge, Edge edge){
        edge.setSourceId(lEdge.getSourceId());
        edge.setTargetId(lEdge.getTargetId());
        edge.setProperty(VERTEX_SIZE_PROPERTY,lEdge.getCount());
        return edge;
      }
    });

    return g.getFactory().fromDataSets(gradoopVertices, gradoopEdges);
  }

  protected static class SimilarityCalculator implements VertexFusor.VertexCompareFunction{
    protected double k;

    public SimilarityCalculator(double k) {
      this.k = k;
    }

    @Override
    public double compare(LVertex v1, LVertex v2) {
      double positionSimilarity =
        Math.max(0,1-(Math.abs(v1.getPosition().distance(v2.getPosition())-k)/k));
      double forceSimilarity = Math.max(0,v1.getForce().normalized().scalar(v2.getForce().normalized()));
      return positionSimilarity*forceSimilarity;
    }
  }

}
