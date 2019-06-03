import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/** Simple class to draw a layouted graph into an image
 *
 */
public class Plotter {

    private BufferedImage img;
    private Graphics gfx;
    private List<Edge> edgeCollection = new ArrayList<Edge>();
    private List<Vertex> vertexCollection = new ArrayList<Vertex>();

    private Options options;

    /** Create a new plotter
     *
     * @param opts Options for this plotter
     */
    public Plotter(Options opts) {
        options = opts;
        img = new BufferedImage(options.width, options.height, BufferedImage.TYPE_INT_RGB);
        gfx = img.getGraphics();
    }

    /** Shortcut for read(), ExecutionEnvironment.execute(), write()
     *
     * @param g  The graph to plot. NEEDS to have properties (X,Y) for the position on EVERY vertex.
     * @param path The path to save the generated file to. The given file-extension determines the used image-type.
     * @return The JobExecutionResult returned by execute()
     */
    public JobExecutionResult plot(LogicalGraph g, String path) throws Exception {
        read(g);
        JobExecutionResult result = g.getVertices().getExecutionEnvironment().execute();
        save(path);
        return result;
    }

    /** Set the plotter up as a DataSink for Flink.
     * Is lazy (does not really compute anything for now).
     *
     * @param g The graph to plot. NEEDS to have properties (X,Y) for the position on EVERY vertex.
     */
    public void read(LogicalGraph g) {
        DataSet<Vertex> vertices = g.getVertices();
        DataSet<Edge> edges = prepareEdges(vertices, g.getEdges());
        edges.output(new LocalCollectionOutputFormat<Edge>(edgeCollection));
        vertices.output(new LocalCollectionOutputFormat<Vertex>(vertexCollection));
    }

    /** Generate and save the image of the graph. HAS TO be called after read() and AFTER the flink-graph has been executed.
     *
     * @param path The path to save the generated file to. The given file-extension determines the used image-type.
     */
    public void save(String path) throws Exception {
        drawImage(vertexCollection, edgeCollection);

        if (options.scaling_mode == Options.Scaling.Scale) {
            img = getScaledImage(img);
        }

        String imgType = getFileExtension(path);
        File outFile = new File(path);
        ImageIO.write(img, imgType, outFile);

        if (options.scaling_mode == Options.Scaling.Copy) {
            img = getScaledImage(img);
            File scaledOutFile = new File(getScaledCopyName(path));
            ImageIO.write(img, imgType, scaledOutFile);
        }
    }

    /** Prepage the given edges for drawing. Assign them start- and end-coordinates from their vertices.
     *
     * @param vertices The vertices to take the edge-coordinates from
     * @param edges The raw edges
     * @return The prepared edges
     */
    protected DataSet<Edge> prepareEdges(DataSet<Vertex> vertices, DataSet<Edge> edges) {
        return edges.join(vertices).where("sourceId").equalTo("id").with(new JoinFunction<Edge, Vertex, Edge>() {
            public Edge join(Edge first, Vertex second) throws Exception {
                first.setProperty("source_x", second.getPropertyValue("X"));
                first.setProperty("source_y", second.getPropertyValue("Y"));
                return first;
            }
        }).join(vertices).where("targetId").equalTo("id").with(new JoinFunction<Edge, Vertex, Edge>() {
            public Edge join(Edge first, Vertex second) throws Exception {
                first.setProperty("target_x", second.getPropertyValue("X"));
                first.setProperty("target_y", second.getPropertyValue("Y"));
                return first;
            }
        });
    }

    /** Do the actual drawing of the graph.
     *
     * @param vertices A list of vertices to draw
     * @param edges A list of edges to draw
     */
    protected void drawImage(List<Vertex> vertices, List<Edge> edges) {
        gfx.setColor(options.edge_color);
        for (Edge e : edges) {
            drawEdge(gfx, e);
        }

        gfx.setColor(options.vertex_color);
        for (Vertex v : vertices) {
            drawVertex(gfx, v);
        }
    }

    /** Get the file-name for the scaled copy of the original image
     *
     * @param original Name of the original image
     * @return Name for the scaled copy
     */
    private String getScaledCopyName(String original) {
        String extension = getFileExtension(original);
        String other = original.substring(0, original.lastIndexOf('.'));
        return other + "-scaled" + extension;
    }

    /** Get the file extension of a file
     *
     * @param path The name/path of the file
     * @return The extension (without dot)
     */
    private String getFileExtension(String path) {
        return path.substring(path.lastIndexOf('.') + 1);
    }

    /** Create a scaled version of the original image.
     * Target-scaling is taken form the options-object
     * this function has been "borrowed" from: https://stackoverflow.com/questions/7951290/re-sizing-an-image-without-losing-quality
     * @param img The original image
     * @return The scaled image
     */
    private BufferedImage getScaledImage(BufferedImage img) {
        int targetWidth = options.scale_width;
        int targetHeight = options.scale_height;
        boolean higherQuality = true;
        Object hint = RenderingHints.VALUE_INTERPOLATION_BICUBIC;
        int type = (img.getTransparency() == Transparency.OPAQUE) ?
                BufferedImage.TYPE_INT_RGB : BufferedImage.TYPE_INT_ARGB;
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

    /** Draw a single edge
     *
     * @param gfx The graphics object to use
     * @param e The edge to draw
     */
    protected void drawEdge(Graphics gfx, Edge e) {
        int sourceX = e.getPropertyValue("source_x").getInt();
        int sourceY = e.getPropertyValue("source_y").getInt();

        int targetX = e.getPropertyValue("target_x").getInt();
        int targetY = e.getPropertyValue("target_y").getInt();

        gfx.drawLine(sourceX + options.vertex_width / 2, sourceY + options.vertex_height / 2, targetX + options.vertex_width / 2, targetY + options.vertex_height / 2);

    }

    /** Draw a single vertex
     *
     * @param gfx The graphics object to use
     * @param v The vertex to draw
     */
    protected void drawVertex(Graphics gfx, Vertex v) {
        int x = v.getPropertyValue("X").getInt();
        int y = v.getPropertyValue("Y").getInt();
        gfx.drawRect(x, y, options.vertex_width, options.vertex_height);
        if (options.vertex_label_name != null) {
            String label = "";
            if (options.vertex_label_name.equals("id")) {
                label = v.getId().toString();
            } else {
                label = v.getPropertyValue(options.vertex_label_name).getString();
            }
            gfx.drawString(label, x, y + (options.vertex_height) + 10);
        }
    }

    /** Bundles all options for the plotter
     *
     */
    public static class Options {
        protected enum Scaling {None, Scale, Copy}

        ;
        protected int width = 500;
        protected int height = 500;
        protected int vertex_width = 20;
        protected int vertex_height = 20;
        protected Color vertex_color = Color.WHITE;
        protected Color edge_color = Color.WHITE;
        protected String vertex_label_name = null;
        protected Scaling scaling_mode = Scaling.None;
        protected int scale_width;
        protected int scale_height;

        /** The dimensions for the drawing-space. Should match (or be larger) then the original layouting-space for the graph.
         *
         * @param w Width in pixels. Default: 500
         * @param h Height in pixels Default: 500
         */
        public Options dimensions(int w, int h) {
            width = w;
            height = h;
            return this;
        }

        /** The drawing-size of the vertices
         *
         * @param w Width in pixels. Default: 20
         * @param h Height in pixels. Default: 20
         */
        public Options vertexSize(int w, int h) {
            vertex_width = w;
            vertex_height = h;
            return this;
        }

        /** The color the vertices should have
         * @param c Default: white
         */
        public Options vertexColor(Color c) {
            vertex_color = c;
            return this;
        }

        /** The color the edges should have
         *
         * @param c Default: white
         */
        public Options edgeColor(Color c) {
            edge_color = c;
            return this;
        }

        /** Name of the vertex-property to draw below the vertex.
         *  'id' = draw the internal id
         *  null = draw noting
         * @param s Default: null
         */
        public Options vertexLabelProperty(String s) {
            vertex_label_name = s;
            return this;
        }

        /** Scale the finished image before saving to save disk-space.
         *
         * @param w Width of the scaled image in pixels.
         * @param h Height of the scaled image in pixels.
         */
        public Options scaleImage(int w, int h) {
            scaling_mode = Scaling.Scale;
            scale_width = w;
            scale_height = h;
            return this;
        }

        /** Save a scaled copy of the output image (but also keep the original).
         * The image is saved with a '-scaled' suffix.
         *
         * @param w Width of the scaled image in pixels.
         * @param h Height of the scaled image in pixels.
         */
        public Options scaleImageCopy(int w, int h) {
            scaleImage(w, h);
            scaling_mode = Scaling.Copy;
            return this;
        }
    }
}
