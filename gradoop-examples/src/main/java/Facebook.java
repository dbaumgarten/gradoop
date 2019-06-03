import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;
import org.gradoop.flink.model.impl.operators.statistics.CrossEdges;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.awt.*;
import java.util.concurrent.TimeUnit;

/** Example that layouts the facebook-graph.
 *  ADJUST THE PATHS BEFORE USE!
 */
public class Facebook {

    static final String OUTPUT_PATH = "/home/daniel/projects/graviz/out/facebook-test.png";
    static final int size = 10000;
    static final int iterations = 100;

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

        LogicalGraphCSVDataSource source = new LogicalGraphCSVDataSource("/home/daniel/projects/graviz/datasets/facebook_gradoop_csv", cfg);


        LayoutingAlgorithm frl = new FRLayouter(FRLayouter.calculateK(size,size, 4100) * 1,
          iterations, size, size, 100);
        LogicalGraph layouted = frl.execute(source.getLogicalGraph());

        Plotter.Options opts =
          new Plotter.Options().dimensions(size, size).vertexSize(5, 5).vertexColor(Color.RED).scaleImageCopy(size / 10, size / 10);
        Plotter p = new Plotter(opts);
        p.read(layouted);

        System.out.println("Crossings: "+new CrossEdges(100).executeLocally(layouted));

        p.save(OUTPUT_PATH);

        System.out.println("Runtime: " + env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS) + "ms");
    }

}
