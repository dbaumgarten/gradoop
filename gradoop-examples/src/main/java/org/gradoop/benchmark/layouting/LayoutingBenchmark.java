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
package org.gradoop.benchmark.layouting;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.benchmark.sampling.SamplingBenchmark;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSink;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouterNaive;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;
import org.gradoop.flink.model.impl.operators.layouting.RandomLayouter;
import org.gradoop.flink.model.impl.operators.layouting.util.Plotter;
import org.gradoop.flink.model.impl.operators.statistics.CrossEdges;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for the graph-layouting
 */
public class LayoutingBenchmark extends AbstractRunner implements ProgramDescription {

  /**
   * Required option to declare the path to the directory containing csv files to be processed.
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare the format of the input data.
   */
  private static final String OPTION_INPUT_FORMAT = "f";
  /**
   * Option to declare the path to output directory.
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to define the to-be-evaluated layouting algorithm.
   * <p>
   * Available mappings:
   * 0 ---> Random
   * 1 ---> FRNaive
   * 2 ---> FR
   */
  private static final String OPTION_SELECTED_ALGORITHM = "a";

  /**
   * Option to ENABLE_PLOTTING the layouted graph
   */
  private static final String OPTION_OUTPUT_FORMAT = "x";
  /**
   * Option to dynamically name the output-directory according to the parameters
   */
  private static final String OPTION_DYNAMIC_OUT = "d";
  /**
   * Option to specify the output-path for the benchmark-results
   */
  private static final String OPTION_BENCHMARK_PATH = "b";
  /**
   * Option to disable Statistics
   */
  private static final String OPTION_NO_STATISTICS = "n";
  /**
   * Option to enable splitting into multiple jobs
   */
  private static final String OPTION_MULTIJOB = "m";
  /**
   * Used input path.
   */
  private static String INPUT_PATH;
  /**
   * Used output path.
   */
  private static String OUTPUT_PATH;
  /**
   * Format of used input data.
   */
  private static String INPUT_FORMAT;
  /**
   * Default format of input data.
   */
  private static final String INPUT_FORMAT_DEFAULT = "csv";
  /**
   * Output path default.
   */
  private static final String OUTPUT_PATH_DEFAULT = "./layouting_benchmark/";
  /**
   * Output path suffix defining where resulting graph sample is written to.
   */
  private static final String OUTPUT_PATH_GRAPH_LAYOUT_SUFFIX = "graph_layout/";
  /**
   * Integer defining the layouting algorithm that is to be evaluated.
   */
  private static int SELECTED_ALGORITHM;
  /**
   * List of parameters that are used to instantiate the selected layouting algorithm.
   */
  private static String[] CONSTRUCTOR_PARAMS;
  /**
   * If true dynamically generate output path
   */
  private static boolean ENABLE_DYNAMIC_OUTPUT_PATH;
  /**
   * Output-format choosen by user
   */
  private static String OUTPUT_FORMAT;
  /**
   * Output path defining where resulting benchmark file is written to.
   */
  private static String OUTPUT_PATH_BENCHMARK = "./benchmark.txt";
  /**
   * If true, do not compute statistics. Only layout and output.
   */
  private static boolean DISABLE_STATISTICS = false;
  /**
   * If true split layouting and statistics(+plotting) in two jobs
   */
  private static boolean MULTIJOB = false;


  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true,
      "Path to directory containing csv files to be processed");
    OPTIONS.addRequiredOption(OPTION_SELECTED_ALGORITHM, "algorithm", true,
      "Positive integer selecting a layouting algorithm");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to directory where resulting graph sample, benchmark file and graph " +
        "statistics are written to. (Defaults to " + OUTPUT_PATH_DEFAULT + ")");
    OPTIONS.addOption(OPTION_INPUT_FORMAT, "format", true,
      "Format of the input data. Defaults to 'csv'");
    OPTIONS.addOption(OPTION_OUTPUT_FORMAT, "outformat", true, "Select output format");
    OPTIONS
      .addOption(OPTION_DYNAMIC_OUT, "dyn", false, "If true include args in output foldername");
    OPTIONS.addOption(OPTION_BENCHMARK_PATH, "benchmarkfile", true,
      "Path where the " + "benchmark-file is written to");
    OPTIONS.addOption(OPTION_NO_STATISTICS, "nostat", false,
      "Disable calculation of statistics. E.g" + ". CrossEdges");
    OPTIONS
      .addOption(OPTION_MULTIJOB, "multijob", false, "Split layouting and statistic in two jobs");
  }

  /**
   * Build the selected LayoutingAlgorithm with the given constuctor parameters
   *
   * @param algo Algorithm to build
   * @param opts A list of options
   * @return The Layouter
   */
  private static LayoutingAlgorithm buildLayoutingAlgorithm(int algo, String[] opts) {
    try {
      int vertexCount = Integer.parseInt(get(opts, 0));
      int width = Integer.parseInt(get(opts, 1));
      int height = Integer.parseInt(get(opts, 2));
      switch (algo) {
      case 0:
        return new RandomLayouter(0, width, 0, height);
      case 1:
        int iterations = Integer.parseInt(get(opts, 3));
        double k = Double.parseDouble(get(opts, 4));
        return new FRLayouterNaive(iterations, vertexCount).area(width, height).k(k);
      case 2:
        iterations = Integer.parseInt(get(opts, 3));
        k = Double.parseDouble(get(opts, 4));
        int grid = Integer.parseInt(get(opts, 5));
        return new FRLayouter(iterations, vertexCount).k(k).maxRepulsionDistance(grid)
          .area(width, height);
      default:
        throw new IllegalArgumentException("Unknown layouting-algorithm: " + algo);
      }
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(
        "Not enogh parameters provided for given algorithm. " + "Found: [" +
          String.join(",", CONSTRUCTOR_PARAMS) + "] and algorithm: " + algo);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        "Expected a number as parameter but found: " + e.getMessage());
    }
  }

  private static String get(String[] arr, int idx) {
    if (arr.length <= idx) {
      return "0";
    }
    return arr[idx];
  }


  /**
   * Main program to run the benchmark. Required arguments are a path to CSVDataSource compatible
   * files that define a graph, an integer defining the sampling algorithm to be tested and a list
   * of parameters for the constructor of the sampling class.
   * Other arguments are the available options.
   *
   * @param args program arguments
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SamplingBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    readCMDArguments(cmd);

    LogicalGraph graph = readLogicalGraph(INPUT_PATH, INPUT_FORMAT);

    // instantiate selected layouting algorithm and create layout
    LayoutingAlgorithm algorithm = buildLayoutingAlgorithm(SELECTED_ALGORITHM, CONSTRUCTOR_PARAMS);
    LogicalGraph layouted = algorithm.execute(graph);

    // write graph sample and benchmark data
    String outpath = OUTPUT_PATH + OUTPUT_PATH_GRAPH_LAYOUT_SUFFIX;
    if (ENABLE_DYNAMIC_OUTPUT_PATH) {
      outpath += getDynamicOutputFolderName() + "/";
    }


    JobExecutionResult jerlayout = null;
    if (MULTIJOB) {
      layouted.writeTo(getDataSink(outpath, "csv", graph.getConfig(), algorithm));
      jerlayout = getExecutionEnvironment().execute("Layouting");
      layouted = readLogicalGraph(outpath, "csv");
    }

    if (!MULTIJOB || !OUTPUT_FORMAT.equals("csv")) {
      layouted.writeTo(getDataSink(outpath, OUTPUT_FORMAT, graph.getConfig(), algorithm));
    }

    Double crossedges = -1d;
    if (!DISABLE_STATISTICS) {
      //This also executes the flink programm as a side-effect
      crossedges = new CrossEdges(CrossEdges.DISABLE_OPTIMIZATION).executeLocally(layouted).f1;
    } else if (!MULTIJOB || !OUTPUT_FORMAT.equals("csv")) {
      getExecutionEnvironment().execute("Output-Conversion");
    }

    if (jerlayout == null) {
      jerlayout = layouted.getConfig().getExecutionEnvironment().getLastJobExecutionResult();
    }

    writeBenchmark(jerlayout, layouted.getConfig().getExecutionEnvironment().getParallelism(),
      algorithm, crossedges);
  }

  /**
   * Returns an EPGM DataSink for a given directory and format.
   *
   * @param directory output path
   * @param format    output format (csv, indexed, json)
   * @param config    gradoop config
   * @return DataSink for EPGM Data
   */
  private static DataSink getDataSink(String directory, String format, GradoopFlinkConfig config,
    LayoutingAlgorithm alg) {
    directory = appendSeparator(directory);
    format = format.toLowerCase();

    switch (format) {
    case "json":
      return new JSONDataSink(directory, config);
    case "csv":
      return new CSVDataSink(directory, config);
    case "indexed":
      return new IndexedCSVDataSink(directory, config);
    case "image":
      int width = Integer.parseInt(CONSTRUCTOR_PARAMS[0]);
      int height = Integer.parseInt(CONSTRUCTOR_PARAMS[1]);
      return new Plotter(directory + "image.png", alg.getWidth(), alg.getHeight(), width, height);
    default:
      throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  /**
   * Reads the given arguments from command line.
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    SELECTED_ALGORITHM = Integer.parseInt(cmd.getOptionValue(OPTION_SELECTED_ALGORITHM));
    CONSTRUCTOR_PARAMS = cmd.getArgList().toArray(new String[0]);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH, OUTPUT_PATH_DEFAULT);
    INPUT_FORMAT = cmd.getOptionValue(OPTION_INPUT_FORMAT, INPUT_FORMAT_DEFAULT);
    ENABLE_DYNAMIC_OUTPUT_PATH = cmd.hasOption(OPTION_DYNAMIC_OUT);
    OUTPUT_FORMAT = cmd.getOptionValue(OPTION_OUTPUT_FORMAT);
    OUTPUT_PATH_BENCHMARK = cmd.getOptionValue(OPTION_BENCHMARK_PATH);
    DISABLE_STATISTICS = cmd.hasOption(OPTION_NO_STATISTICS);
    MULTIJOB = cmd.hasOption(OPTION_MULTIJOB);
  }

  /**
   * Generates a folder name from the input arguments
   *
   * @return A foldername
   */
  private static String getDynamicOutputFolderName() {
    return SELECTED_ALGORITHM + "-" + String.join("-", CONSTRUCTOR_PARAMS);
  }

  /**
   * Method to crate and add lines to a benchmark file.
   *
   * @param result      The JoExecutionResult for the layouting
   * @param parallelism Parallelism level used for the layouting
   * @param layouting   layouting algorithm under test
   * @param crossedges  number of detected edge-crossings
   * @throws IOException exception during file writing
   */
  private static void writeBenchmark(JobExecutionResult result, int parallelism,
    LayoutingAlgorithm layouting, double crossedges) throws IOException {
    String head = String
      .format("%s|%s|%s|%s|%s|%s%n", "Parallelism", "Dataset", "Algorithm", "Params",
        "Runtime " + "[s]", "Crossedges");

    // build log
    String layoutingName = layouting.getClass().getSimpleName();
    String tail = String.format("%s|%s|%s|%s|%s|%s%n", parallelism,
      INPUT_PATH.substring(INPUT_PATH.lastIndexOf(File.separator) + 1), layoutingName,
      String.join(", ", layouting.toString()), result.getNetRuntime(TimeUnit.SECONDS), crossedges);

    File f = new File(OUTPUT_PATH_BENCHMARK);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(OUTPUT_PATH_BENCHMARK, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }

  @Override
  public String getDescription() {
    return this.getClass().getName();
  }
}
