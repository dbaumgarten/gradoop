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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;
import org.gradoop.flink.model.impl.operators.layouting.util.Plotter;
import org.gradoop.flink.model.impl.operators.statistics.CrossEdges;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.concurrent.TimeUnit;

/**
 * Example that layouts the facebook-graph.
 * ADJUST THE PATHS BEFORE USE!
 */
public class Facebook {

  /** foo */
  private static String OUTPUT_PATH = System.getProperty("user.dir") + "/out/facebook-test.png";
  /** foo */
  private static String INPUT_PATH = System.getProperty("user.dir") + "/datasets" +
    "/facebook_gradoop_csv";
  /** foo */
  private static int ITERATIONS = 25;

  /** foo
   *
   * @param args bar
   * @throws Exception baz
   */
  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

    LogicalGraphCSVDataSource source = new LogicalGraphCSVDataSource(INPUT_PATH, cfg);
    LayoutingAlgorithm frl = new FRLayouter(ITERATIONS, 4100);
    System.out.println(frl);
    LogicalGraph layouted = frl.execute(source.getLogicalGraph());

    Plotter p = new Plotter(OUTPUT_PATH, frl, 1000, 1000).edgeSize(0.1f).ignoreVertices(true);

    layouted.writeTo(p);

    //env.execute();

    System.out.println(
      "Crossings: " + new CrossEdges(CrossEdges.DISABLE_OPTIMIZATION).executeLocally(layouted));

    System.out.println(
      "Runtime: " + env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS) + "ms");
  }

}
