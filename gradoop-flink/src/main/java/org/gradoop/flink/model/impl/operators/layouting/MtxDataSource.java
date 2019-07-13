package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class MtxDataSource implements DataSource {

  private String path;
  private GradoopFlinkConfig cfg;
  private boolean skipPreprocessing;

  public MtxDataSource(String path, GradoopFlinkConfig cfg) {
    this.path = path;
    this.cfg = cfg;
  }

  public MtxDataSource(String path, GradoopFlinkConfig cfg, boolean skipPreprocessing) {
    this.path = path;
    this.cfg = cfg;
    this.skipPreprocessing = skipPreprocessing;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    DataSet<Vertex> vertices =
      cfg.getExecutionEnvironment().readTextFile(path).flatMap(new VertexMapper(cfg.getVertexFactory())).distinct("id");

    DataSet<Edge> edges =
      cfg.getExecutionEnvironment().readTextFile(path).flatMap(new EdgeMapper(cfg.getEdgeFactory()));

    if (!skipPreprocessing){
      edges = edges.filter((e)->!e.getSourceId().equals(e.getTargetId()));
      edges = edges.map(e->{
        if (e.getSourceId().compareTo(e.getTargetId()) > 0){
          GradoopId old = e.getSourceId();
          e.setSourceId(e.getTargetId());
          e.setTargetId(old);
        }
        return e;
      }).distinct("sourceId", "targetId");
    }

    return cfg.getLogicalGraphFactory().fromDataSets(vertices,edges);
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    throw new NotImplementedException();
  }

  private static boolean isComment(String line){
    return line.startsWith("#") || line.startsWith("%");
  }

  private static String getSplitCharacter(String text){
    return text.contains(" ")?" ":"\t";
  }

  private static GradoopId generateId(String text){
    String hex = String.format("%24s",  Integer.toHexString(Integer.parseInt(text))).replace(' ', '0');
    return GradoopId.fromString(hex);
  }

  private static class EdgeMapper implements FlatMapFunction<String,Edge> {
    private EPGMEdgeFactory<Edge> edgeFactory;

    public EdgeMapper(EPGMEdgeFactory<Edge> edgeFactory) {
      this.edgeFactory = edgeFactory;
    }

    @Override
    public void flatMap(String line, Collector<Edge> collector) {
      if(!isComment(line)){
        String[] splitted = line.split(getSplitCharacter(line));
        collector.collect(edgeFactory.initEdge(GradoopId.get(),
          "edge",
          generateId(splitted[0]),
          generateId(splitted[1])));
      }
    }
  }

  private static class VertexMapper implements FlatMapFunction<String,Vertex> {
    private EPGMVertexFactory<Vertex> vertexFactory;

    public VertexMapper(EPGMVertexFactory<Vertex> vertexFactory) {
      this.vertexFactory = vertexFactory;
    }

    @Override
    public void flatMap(String line, Collector<Vertex> collector) {
      if(!isComment(line)){
        String[] splitted = line.split(getSplitCharacter(line));
        collector.collect(vertexFactory.initVertex(generateId(splitted[0]),"vertex"));
        collector.collect(vertexFactory.initVertex(generateId(splitted[1]),"vertex"));
      }
    }
  }
}
