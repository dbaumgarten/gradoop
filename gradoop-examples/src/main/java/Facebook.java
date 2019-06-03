import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;
import org.gradoop.flink.model.impl.operators.statistics.CrossEdges;
import org.gradoop.flink.model.impl.operators.statistics.CrossEdgesNew;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.neo4j.cypher.internal.javacompat.ExecutionResult;

import java.util.List;
import java.awt.Color;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/** Example that layouts the facebook-graph.
 *  ADJUST THE PATHS BEFORE USE!
 */
public class Facebook {

    static final String OUTPUT_PATH = "/home/daniel/projects/graviz/out/facebook-test.png";
    static final int size = 10000;
    static final int iterations = 10;

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

        LogicalGraphCSVDataSource source = new LogicalGraphCSVDataSource("/home/daniel/projects/graviz/datasets/facebook_gradoop_csv", cfg);


        LayoutingAlgorithm frl = new FRLayouter(FRLayouter.calculateK(size,size, 4100) * 1,
          iterations, size, size, 100);
        LogicalGraph layouted = frl.execute(source.getLogicalGraph());

        DataSet<Tuple2<Integer,Double>> stat = new CrossEdgesNew(100).execute(layouted);
        /*List<Tuple2<Integer,Double>> stats = new ArrayList<>();
        stat.output(new LocalCollectionOutputFormat<Tuple2<Integer,Double>>(stats));

        Plotter.Options opts =
          new Plotter.Options().dimensions(size, size).vertexSize(5, 5).vertexColor(Color.RED).scaleImageCopy(size / 10, size / 10);
        Plotter p = new Plotter(opts);
        JobExecutionResult result = p.plot(layouted,OUTPUT_PATH);
        System.out.println("Runtime: " + result.getNetRuntime(TimeUnit.MILLISECONDS) + "ms");
        System.out.println("Crossings: "+stats.get(0));

         */
        System.out.println(stat.collect().get(0));

    }

}
