import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;
import org.gradoop.flink.model.impl.operators.layouting.util.Plotter;
import org.gradoop.flink.model.impl.operators.statistics.CrossEdges;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.concurrent.TimeUnit;

/** Example that layouts the facebook-graph.
 *  ADJUST THE PATHS BEFORE USE!
 */
public class Facebook {

    static String OUTPUT_PATH = System.getProperty("user.dir")+"/out/facebook-test.png";
    static String INPUT_PATH = System.getProperty("user.dir")+"/datasets/facebook_gradoop_csv";
    static final int size = 10000;
    static final int iterations = 25;

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

        LogicalGraphCSVDataSource source = new LogicalGraphCSVDataSource(INPUT_PATH, cfg);
        double k = FRLayouter.calculateK(size,size, 4100) * 5;
        System.out.println("K is: "+k);
        LayoutingAlgorithm frl = new FRLayouter(k,
          iterations, size, size, 300);
        LogicalGraph layouted = frl.execute(source.getLogicalGraph());

        Plotter p =
          new Plotter(new Plotter.Options().dimensions(size, size).ignoreVertices(true).scaleImage(size/10,size/10),
            OUTPUT_PATH);

        layouted.writeTo(p);

        System.out.println("Crossings: "+new CrossEdges(100).executeLocally(layouted));

        System.out.println("Runtime: " + env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS) + "ms");
    }

}
