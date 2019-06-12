package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * DataSink to write a layouted graph to an image
 */

public class Plotter implements DataSink {

  private Options options;
  private String path;

  public Plotter(Options opts, String path) {
    options = opts;
    this.path = path;
  }

  /**
   * Prepage the given edges for drawing. Assign them start- and end-coordinates from their
   * vertices.
   *
   * @param vertices The vertices to take the edge-coordinates from
   * @param edges    The raw edges
   * @return The prepared edges
   */
  public DataSet<Edge> prepareEdges(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    final Options options = this.options;
    return edges.join(vertices).where("sourceId").equalTo("id")
      .with(new JoinFunction<Edge, Vertex, Edge>() {
        public Edge join(Edge first, Vertex second) throws Exception {
          first.setProperty("source_x", second.getPropertyValue("X"));
          first.setProperty("source_y", second.getPropertyValue("Y"));
          if(options.vertex_label != null){
            first.setProperty("source_label", second.getPropertyValue(options.vertex_label));
          }
          return first;
        }
      }).join(vertices).where("targetId").equalTo("id")
      .with(new JoinFunction<Edge, Vertex, Edge>() {
        public Edge join(Edge first, Vertex second) throws Exception {
          first.setProperty("target_x", second.getPropertyValue("X"));
          first.setProperty("target_y", second.getPropertyValue("Y"));
          if(options.vertex_label != null){
            first.setProperty("target_label", second.getPropertyValue(options.vertex_label));
          }
          return first;
        }
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
    DataSet<Edge> edges = prepareEdges(logicalGraph.getVertices(),logicalGraph.getEdges());

    PlotterOutputFormat pof = new PlotterOutputFormat(options,path);
    FileSystem.WriteMode writeMode =
      overwrite ? FileSystem.WriteMode.OVERWRITE :  FileSystem.WriteMode.NO_OVERWRITE;
    pof.setWriteMode(writeMode);
    edges.output(pof).setParallelism(1);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overwrite) throws IOException {
    throw new UnsupportedOperationException("Plotting is not supported for GraphCollections");
  }

  protected static class PlotterOutputFormat extends FileOutputFormat<Edge> {

    transient private BufferedImage img;
    transient private Graphics gfx;
    private String path;
    private Options options;
    private Set<Tuple3<Integer,Integer,String>> vertices;

    /**
     * Create a new plotter
     *
     * @param opts Options for this plotter
     */
    public PlotterOutputFormat(Options opts, String path) {
      super(new Path(path));
      options = opts;
      this.path = path;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
      super.open(taskNumber, numTasks);
      img = new BufferedImage(options.width, options.height, BufferedImage.TYPE_INT_RGB);
      vertices = new HashSet<>();
      gfx = img.getGraphics();
      gfx.setColor(options.edge_color);
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

    /**
     * Create a scaled version of the original image.
     * Target-scaling is taken form the options-object
     * this function has been "borrowed" from: https://stackoverflow
     * .com/questions/7951290/re-sizing-an-image-without-losing-quality
     *
     * @param img The original image
     * @return The scaled image
     */
    private BufferedImage getScaledImage(BufferedImage img) {
      int targetWidth = options.scale_width;
      int targetHeight = options.scale_height;
      boolean higherQuality = true;
      Object hint = RenderingHints.VALUE_INTERPOLATION_BICUBIC;
      int type = (img.getTransparency() == Transparency.OPAQUE) ? BufferedImage.TYPE_INT_RGB :
        BufferedImage.TYPE_INT_ARGB;
      BufferedImage ret = (BufferedImage) img;
      int w, h;
      if (higherQuality) {
        w = img.getWidth();
        h = img.getHeight();
      } else {
        w = targetWidth;
        h = targetHeight;
      }

      do {
        if (higherQuality && w > targetWidth) {
          w /= 2;
          if (w < targetWidth) {
            w = targetWidth;
          }
        }

        if (higherQuality && h > targetHeight) {
          h /= 2;
          if (h < targetHeight) {
            h = targetHeight;
          }
        }

        BufferedImage tmp = new BufferedImage(w, h, type);
        Graphics2D g2 = tmp.createGraphics();
        g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, hint);
        g2.drawImage(ret, 0, 0, w, h, null);
        g2.dispose();

        ret = tmp;
      } while (w != targetWidth || h != targetHeight);

      return ret;
    }

    /**
     * Draw a single edge
     *
     * @param e   The edge to draw
     */
    protected void drawEdge(Edge e) {
      gfx.setColor(options.edge_color);
      try {
        int sourceX = e.getPropertyValue("source_x").getInt();
        int sourceY = e.getPropertyValue("source_y").getInt();

        int targetX = e.getPropertyValue("target_x").getInt();
        int targetY = e.getPropertyValue("target_y").getInt();

        gfx.drawLine(sourceX + options.vertex_width / 2, sourceY + options.vertex_height / 2,
          targetX + options.vertex_width / 2, targetY + options.vertex_height / 2);
      }catch (NullPointerException ef){

      }
    }

    /**
     * Draw a single vertex
     *
     */
    protected void drawVertex(int x, int y, String label) {
      gfx.drawRect(x, y, options.vertex_width, options.vertex_height);
      if (label != null) {
        gfx.drawString(label, x, y + (options.vertex_height) + 10 + (options.vertex_label_size/2));
      }
    }

    @Override
    public void writeRecord(Edge edge){
      drawEdge(edge);
      if (!options.ignoreVertices) {
        vertices.add(new Tuple3<>(edge.getPropertyValue("source_x").getInt(),
          edge.getPropertyValue("source_y").getInt(),(options.vertex_label != null)?
          edge.getPropertyValue("source_label").getString():null));
        vertices.add(new Tuple3<>(edge.getPropertyValue("target_x").getInt(),
          edge.getPropertyValue("target_y").getInt(),(options.vertex_label != null)?
          edge.getPropertyValue("target_label").getString():null));
      }
    }

    @Override
    public void close() throws IOException {
      if (!options.ignoreVertices) {
        gfx.setColor(options.vertex_color);
        gfx.setFont(new Font("Arial",Font.PLAIN,options.vertex_label_size));
        for (Tuple3<Integer, Integer,String> t : vertices) {
          drawVertex(t.f0, t.f1, t.f2);
        }
      }
      if (options.scale) {
        img = getScaledImage(img);
      }
      ImageIO.write(img,getFileExtension(path),this.stream);
      super.close();
    }

  }



  /**
   * Bundles all options for the plotter
   */
  public static class Options implements Serializable {

    protected int width = 1000;
    protected int height = 1000;
    protected int vertex_width = 10;
    protected int vertex_height = 10;
    protected Color vertex_color = Color.RED;
    protected Color edge_color = Color.WHITE;
    protected boolean scale = false;
    protected int scale_width;
    protected int scale_height;
    protected boolean ignoreVertices = false;
    protected String vertex_label;
    protected int vertex_label_size = 10;

    /**
     * The dimensions for the drawing-space. Should match (or be larger) then the original
     * layouting-space for the graph.
     *
     * @param w Width in pixels. Default: 500
     * @param h Height in pixels Default: 500
     */
    public Options dimensions(int w, int h) {
      width = w;
      height = h;
      return this;
    }

    /**
     * The drawing-size of the vertices
     *
     * @param w Width in pixels. Default: 20
     * @param h Height in pixels. Default: 20
     */
    public Options vertexSize(int w, int h) {
      vertex_width = w;
      vertex_height = h;
      return this;
    }

    /**
     * The color the vertices should have
     *
     * @param c Default: white
     */
    public Options vertexColor(Color c) {
      vertex_color = c;
      return this;
    }

    /**
     * The color the edges should have
     *
     * @param c Default: white
     */
    public Options edgeColor(Color c) {
      edge_color = c;
      return this;
    }

    /**
     * Scale the finished image before saving to save disk-space.
     *
     * @param w Width of the scaled image in pixels.
     * @param h Height of the scaled image in pixels.
     */
    public Options scaleImage(int w, int h) {
      scale = true;
      scale_width = w;
      scale_height = h;
      return this;
    }

    /** If true vertices are not drawn. Increases performance.
     *
     * @param b true/false
     * @return this
     */
    public Options ignoreVertices(boolean b){
      ignoreVertices = b;
      return this;
    }

    /** Name of the vertex-property to write below a vertex
     *
     * @param prop Property-name. Default: null
     * @return this
     */
    public Options vertexLabelProperty(String prop){
      vertex_label = prop;
      return this;
    }

    /** Set the font-size of the vertex labels
     *
     * @param size Font size. Default: 10
     * @return this
     */
    public Options vertexLabelSize(int size){
      vertex_label_size = size;
      return this;
    }

  }
}
