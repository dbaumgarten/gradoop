package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;

import javax.imageio.ImageIO;
import java.awt.*;
import java.util.List;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.Serializable;

/**
 * DataSink to write a layouted graph to an image
 */

public class Plotter implements DataSink, Serializable {

  protected String path;
  protected int layoutWidth;
  protected int layoutHeight;
  protected int imageWidth;
  protected int imageHeight;

  protected int vertexSize = 10;
  protected float edgeSize = 1f;
  protected Color vertexColor = Color.RED;
  protected Color edgeColor = Color.WHITE;
  protected Color backgroundColor = Color.BLACK;
  protected boolean ignoreVertices = false;
  protected String vertexLabel = null;
  protected int vertexLabelSize = 10;

  public Plotter(String path, int layoutWidth, int layoutHeight, int imageWidth, int imageHeight) {
    this.path = path;
    this.layoutWidth = layoutWidth;
    this.layoutHeight = layoutHeight;
    this.imageWidth = imageWidth;
    this.imageHeight = imageHeight;
  }

  public Plotter(String path, LayoutingAlgorithm algo, int imageWidth, int imageHeight) {
    this.path = path;
    this.layoutWidth = algo.getWidth();
    this.layoutHeight = algo.getHeight();
    this.imageWidth = imageWidth;
    this.imageHeight = imageHeight;
  }

  /**
   * Sets optional value vertexSize
   *
   * @param vertexSize the new value
   * @return this (for method-chaining)
   */
  public Plotter vertexSize(int vertexSize) {
    this.vertexSize = vertexSize;
    return this;
  }

  /**
   * Sets optional value vertexColor
   *
   * @param vertexColor the new value
   * @return this (for method-chaining)
   */
  public Plotter vertexColor(Color vertexColor) {
    this.vertexColor = vertexColor;
    return this;
  }

  /**
   * Sets optional value edgeColor
   *
   * @param edgeColor the new value
   * @return this (for method-chaining)
   */
  public Plotter edgeColor(Color edgeColor) {
    this.edgeColor = edgeColor;
    return this;
  }

  /**
   * Sets optional value ignoreVertices
   *
   * @param ignoreVertices the new value
   * @return this (for method-chaining)
   */
  public Plotter ignoreVertices(boolean ignoreVertices) {
    this.ignoreVertices = ignoreVertices;
    return this;
  }

  /**
   * Sets optional value vertexLabel
   *
   * @param vertexLabel the new value
   * @return this (for method-chaining)
   */
  public Plotter vertexLabel(String vertexLabel) {
    this.vertexLabel = vertexLabel;
    return this;
  }

  /**
   * Sets optional value vertexLabelSize
   *
   * @param vertexLabelSize the new value
   * @return this (for method-chaining)
   */
  public Plotter vertexLabelSize(int vertexLabelSize) {
    this.vertexLabelSize = vertexLabelSize;
    return this;
  }

  /**
   * Sets optional value backgroundColor
   *
   * @param backgroundColor the new value
   * @return this (for method-chaining)
   */
  public Plotter backgroundColor(Color backgroundColor) {
    this.backgroundColor = backgroundColor;
    return this;
  }

  /**
   * Sets optional value edgeSize
   *
   * @param edgeSize the new value
   * @return this (for method-chaining)
   */
  public Plotter edgeSize(float edgeSize) {
    this.edgeSize = edgeSize;
    return this;
  }

  /**
   * Prepare the given edges for drawing. Assign them start- and end-coordinates from their
   * vertices.
   *
   * @param vertices The vertices to take the edge-coordinates from
   * @param edges    The raw edges
   * @return The prepared edges
   */
  protected DataSet<Edge> prepareEdges(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    edges = edges.join(vertices).where("sourceId").equalTo("id")
      .with(new JoinFunction<Edge, Vertex, Edge>() {
        public Edge join(Edge first, Vertex second) throws Exception {
          first.setProperty("source_x", second.getPropertyValue("X"));
          first.setProperty("source_y", second.getPropertyValue("Y"));
          return first;
        }
      }).join(vertices).where("targetId").equalTo("id")
      .with(new JoinFunction<Edge, Vertex, Edge>() {
        public Edge join(Edge first, Vertex second) throws Exception {
          first.setProperty("target_x", second.getPropertyValue("X"));
          first.setProperty("target_y", second.getPropertyValue("Y"));
          return first;
        }
      });
   return edges;
  }

  /** Scale the coordinates of the graph so that the layout-space matches the requested drawing-size
   *
   * @param inp original vertices
   * @return vertices with scaled coordinates
   */
  protected DataSet<Vertex> scaleLayout(DataSet<Vertex> inp){
    final double widthScale = imageWidth/(double)layoutHeight;
    final double heightScale = imageHeight/(double)layoutHeight;
    return inp.map((v)->{
      int x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
      int y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
      x = (int) (x * widthScale);
      y = (int) (y * heightScale);
      v.setProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY,x);
      v.setProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY,y);
      return v;
    });
  }


  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph,true);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection,true);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {

    ImageOutputFormat pof = new ImageOutputFormat(path);
    FileSystem.WriteMode writeMode =
      overwrite ? FileSystem.WriteMode.OVERWRITE :  FileSystem.WriteMode.NO_OVERWRITE;
    pof.setWriteMode(writeMode);

    DataSet<Vertex> vertices = scaleLayout(logicalGraph.getVertices());
    DataSet<Edge> edges = prepareEdges(vertices, logicalGraph.getEdges());

    ImageGenerator imgg = new ImageGenerator(this);
    DataSet<BufferedImage> image = edges.combineGroup(imgg::combineEdges).reduce(imgg::mergeImages);
    if (!ignoreVertices){
      DataSet<BufferedImage> vertexImage =
        vertices.combineGroup(imgg::combineVertices).reduce(imgg::mergeImages);
      image = image.map(new RichMapFunction<BufferedImage, BufferedImage>() {
        @Override
        public BufferedImage map(BufferedImage bufferedImage) throws Exception {
          List<BufferedImage> vertexImage = this.getRuntimeContext().getBroadcastVariable(
            "vertexImage");
          return imgg.mergeImages(bufferedImage,vertexImage.get(0));
        }
      }).withBroadcastSet(vertexImage,"vertexImage");
    }
    image = image.map(imgg::addBackgound);

    image.output(pof).setParallelism(1);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overwrite) throws IOException {
    throw new UnsupportedOperationException("Plotting is not supported for GraphCollections");
  }

  /** This class contains functionality to create images from graph-parts
   *
   */
  protected static class ImageGenerator implements Serializable {

    /** Contains all necessary parameters */
    private Plotter plotter;

    /** Create new image-generatir
     *
     * @param p Contains all necessary parameters (cannot use non-static class du to flink-madness)
     */
    public ImageGenerator(Plotter p){
        this.plotter = p;
    }


    /** Combine multiple edges into one Image
     *
     * @param iterable The edges to combine
     * @param collector The output-collector
     */
    public void combineEdges(Iterable<Edge> iterable, Collector<BufferedImage> collector) {
      BufferedImage img = new BufferedImage(plotter.imageWidth, plotter.imageHeight,
        BufferedImage.TYPE_INT_ARGB);
      Graphics2D gfx = img.createGraphics();
      gfx.setColor(plotter.edgeColor);
      gfx.setRenderingHint(RenderingHints.KEY_ANTIALIASING,RenderingHints.VALUE_ANTIALIAS_ON);
      gfx.setStroke(new BasicStroke(plotter.edgeSize));
      for(Edge e: iterable){
        drawEdge(gfx,e);
      }
      collector.collect(img);
      gfx.dispose();
    }

    /** Combine multiple vertices into one Image
     *
     * @param iterable The vertices to combine
     * @param collector The output-collector
     */
    public void combineVertices(Iterable<Vertex> iterable, Collector<BufferedImage> collector) {
      BufferedImage img = new BufferedImage(plotter.imageWidth, plotter.imageHeight,
        BufferedImage.TYPE_INT_ARGB);
      Graphics2D gfx = img.createGraphics();
      gfx.setColor(plotter.vertexColor);
      for(Vertex v: iterable){
        drawVertex(gfx,v);
      }
      collector.collect(img);
      gfx.dispose();
    }

    /**
     * Draw a single edge
     *
     * @param gfx The graphics-object to use for drawing
     * @param e   The edge to draw
     */
    private void drawEdge(Graphics2D gfx, Edge e) {
      gfx.setColor(plotter.edgeColor);
      try {
        int sourceX = e.getPropertyValue("source_x").getInt();
        int sourceY = e.getPropertyValue("source_y").getInt();

        int targetX = e.getPropertyValue("target_x").getInt();
        int targetY = e.getPropertyValue("target_y").getInt();

        gfx.drawLine(sourceX + plotter.vertexSize / 2, sourceY + plotter.vertexSize / 2,
          targetX + plotter.vertexSize / 2, targetY + plotter.vertexSize / 2);
      }catch (NullPointerException ef){

      }
    }

    /**
     * Draw a single vertex
     *
     * @param gfx The graphics-object to use for drawing
     * @param v The vertex to draw
     *
     */
    private void drawVertex(Graphics2D gfx, Vertex v) {
      int x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
      int y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
      gfx.drawRect(x, y, plotter.vertexSize, plotter.vertexSize);
      if (plotter.vertexLabel != null) {
        String label = v.getPropertyValue(plotter.vertexLabel).getString();
        gfx.drawString(label, x,
          y + (plotter.vertexSize) + 10 + (plotter.vertexLabelSize/2));
      }
    }

    /** Merge two intermediate Images into one
     *
     * @param bufferedImage Image 1
     * @param t1 Image 2
     * @return Output-Image
     */
    public BufferedImage mergeImages(BufferedImage bufferedImage, BufferedImage t1) {
      Graphics g = bufferedImage.getGraphics();
      g.drawImage(t1,0,0,plotter.imageWidth,plotter.imageHeight,null);
      g.dispose();
      return bufferedImage;
    }

    /** Draw a black background behind the image
     *
     * @param bufferedImage Input image
     * @return Input-image + black background
     * @throws Exception
     */
    public BufferedImage addBackgound(BufferedImage bufferedImage) throws Exception {
      BufferedImage out = new BufferedImage(plotter.imageWidth,plotter.imageHeight,BufferedImage.TYPE_INT_ARGB);
      Graphics2D gfx = out.createGraphics();
      gfx.setColor(plotter.backgroundColor);
      gfx.fillRect(0,0,plotter.imageWidth,plotter.imageHeight);
      gfx.drawImage(bufferedImage,0,0,plotter.imageWidth,plotter.imageHeight,null);
      gfx.dispose();
      return out;
    }
  }

  /** OutputFormat to save BufferedImages to image files
   *
   */
  protected static class ImageOutputFormat extends FileOutputFormat<BufferedImage>  {

    private String path;

    /**
     * Create a new plotter output format
     *
     * @param path The output-image location
     */
    public ImageOutputFormat(String path) {
      super(new Path(path));
      this.path = path;
    }



    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
      super.open(taskNumber, numTasks);
    }

    /**
     * Get the file extension of a file
     *
     * @param path The name/path of the file
     * @return The extension (without dot)
     */
    private String getFileExtension(String path) {
      return path.substring(path.lastIndexOf('.') + 1);
    }


    @Override
    public void writeRecord(BufferedImage img) throws IOException{
      ImageIO.write(img,getFileExtension(path),this.stream);
    }

  }

}
