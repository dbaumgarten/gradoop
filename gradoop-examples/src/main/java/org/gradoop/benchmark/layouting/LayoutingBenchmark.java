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
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSink;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSource;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSink;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSource;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphIndexedCSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.CentroidFRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.FRLayouterNaive;
import org.gradoop.flink.model.impl.operators.layouting.FusingFRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.GiLaLayouter;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;
import org.gradoop.flink.model.impl.operators.layouting.MtxDataSource;
import org.gradoop.flink.model.impl.operators.layouting.RandomLayouter;
import org.gradoop.flink.model.impl.operators.layouting.SamplingFRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.util.Plotter;
import org.gradoop.flink.model.impl.operators.statistics.CrossEdges;
import org.gradoop.flink.model.impl.operators.statistics.EdgeLengthDerivation;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
   * Option to specify a statistic to compute
   */
  private static final String OPTION_STATISTIC = "s";
  /**
   * Option to enable splitting into multiple jobs
   */
  private static final String OPTION_MULTIJOB = "m";
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
   * The statistic that shall be computed
   */
  private static String STATISTIC = "";
  /**
   * If true split layouting and statistics(+plotting) in two jobs
   */
  private static boolean MULTIJOB = false;


  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true,
      "Path to directory containing csv files to be processed");
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
    OPTIONS.addOption(OPTION_STATISTIC, "statistic", true,
      "Choose a statistic to compute for the layout. (cre or eld)");
    OPTIONS
      .addOption(OPTION_MULTIJOB, "multijob", false, "Split layouting and statistic in two jobs");
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
    LayoutingAlgorithm algorithm = buildLayoutingAlgorithm(CONSTRUCTOR_PARAMS,
      (int)graph.getVertices().count());

    System.out.println("----------");
    System.out.println("INPUT: "+new File(INPUT_PATH).getName());
    System.out.println("PARALLELISM: "+getExecutionEnvironment().getParallelism());
    System.out.println("ALGO: "+algorithm);
    System.out.println("----------");

    LogicalGraph layouted = algorithm.execute(graph);

    // write graph sample and benchmark data
    String outpath = OUTPUT_PATH + OUTPUT_PATH_GRAPH_LAYOUT_SUFFIX;
    if (ENABLE_DYNAMIC_OUTPUT_PATH) {
      outpath += getDynamicOutputFolderName() + "/";
    }

    JobExecutionResult layoutExecutionEnvironment = null;
    if (MULTIJOB) {
      layouted.writeTo(getDataSink(outpath, "csv", graph.getConfig(), algorithm),true);
      layoutExecutionEnvironment = getExecutionEnvironment().execute("Layouting");
      layouted = readLogicalGraph(outpath, "csv");
    }

    if (!MULTIJOB || !OUTPUT_FORMAT.equals("csv")) {
      layouted.writeTo(getDataSink(outpath, OUTPUT_FORMAT, graph.getConfig(), algorithm),true);
    }

    Double statisticValue;
    switch (STATISTIC) {
    case "cre":
      //This also executes the flink program as a side-effect
      statisticValue = new CrossEdges(CrossEdges.DISABLE_OPTIMIZATION).executeLocally(layouted).f1;
      break;
    case "eld":
      //This also executes the flink program as a side-effect
      statisticValue = new EdgeLengthDerivation().execute(layouted).collect().get(0);
      break;
    default:
      statisticValue = 0d;
      if (!MULTIJOB || !OUTPUT_FORMAT.equals("csv")) {
        getExecutionEnvironment().execute("Output-Conversion");
      }
      break;
    }

    if (layoutExecutionEnvironment == null) {
      layoutExecutionEnvironment =
        layouted.getConfig().getExecutionEnvironment().getLastJobExecutionResult();
    }

    writeBenchmark(layoutExecutionEnvironment,
      layouted.getConfig().getExecutionEnvironment().getParallelism(), algorithm, statisticValue);
  }

  /**
   * Reads the given arguments from command line.
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    CONSTRUCTOR_PARAMS = cmd.getArgList().toArray(new String[0]);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH, OUTPUT_PATH_DEFAULT);
    INPUT_FORMAT = cmd.getOptionValue(OPTION_INPUT_FORMAT, INPUT_FORMAT_DEFAULT);
    ENABLE_DYNAMIC_OUTPUT_PATH = cmd.hasOption(OPTION_DYNAMIC_OUT);
    OUTPUT_FORMAT = cmd.getOptionValue(OPTION_OUTPUT_FORMAT);
    OUTPUT_PATH_BENCHMARK = cmd.getOptionValue(OPTION_BENCHMARK_PATH);
    STATISTIC = cmd.getOptionValue(OPTION_STATISTIC);
    MULTIJOB = cmd.hasOption(OPTION_MULTIJOB);
  }

  /**
   * Build the selected LayoutingAlgorithm with the given constructor parameters
   *
   * @param opts A list of options
   * @return The layouter
   */
  private static LayoutingAlgorithm buildLayoutingAlgorithm(String[] opts, int vertexcount) {

    LayoutingAlgorithm algo = null;
    try {
      if (opts.length == 0) {
        throw new IllegalArgumentException("Please specify an algorithm to use.");
      }

      String algoname = opts[0];

      switch (algoname) {
      case "RandomLayouter":
        if (opts.length != 5) {
          throw new IllegalArgumentException("Selected algorithm needs exactly 4 arguments");
        }
        int minX = Integer.parseInt(opts[1]);
        int maxX = Integer.parseInt(opts[2]);
        int minY = Integer.parseInt(opts[3]);
        int maxY = Integer.parseInt(opts[4]);
        algo = new RandomLayouter(minX, maxX, minY, maxY);
        applyOptionalArguments(algo, 5);
        break;
      case "FRLayouterNaive":
        if (opts.length < 2) {
          throw new IllegalArgumentException("Selected algorithm has 1 required arguments");
        }
        int iterations = Integer.parseInt(opts[1]);
        algo = new FRLayouterNaive(iterations, vertexcount);
        applyOptionalArguments(algo, 2);
        break;
      case "FRLayouter":
        if (opts.length < 2) {
          throw new IllegalArgumentException("Selected algorithm has 1 required arguments");
        }
        iterations = Integer.parseInt(opts[1]);
        algo = new FRLayouter(iterations, vertexcount);
        applyOptionalArguments(algo, 2);
        break;
      case "SamplingFRLayouter":
        if (opts.length < 3) {
          throw new IllegalArgumentException("Selected algorithm has 2 required arguments");
        }
        iterations = Integer.parseInt(opts[1]);
        double rate = Double.parseDouble(opts[2]);
        algo = new SamplingFRLayouter(iterations, vertexcount, rate);
        applyOptionalArguments(algo, 3);
        break;
      case "GiLaLayouter":
        if (opts.length < 3) {
          throw new IllegalArgumentException("Selected algorithm has 2 required arguments");
        }
        iterations = Integer.parseInt(opts[1]);
        int kNeighborhood = Integer.parseInt(opts[2]);
        algo = new GiLaLayouter(iterations, vertexcount, kNeighborhood);
        applyOptionalArguments(algo, 3);
        break;
      case "FusingFRLayouter":
        if (opts.length < 4) {
          throw new IllegalArgumentException("Selected algorithm has 3 required arguments");
        }
        iterations = Integer.parseInt(opts[1]);
        rate = Double.parseDouble(opts[2]);
        algo = new FusingFRLayouter(iterations, vertexcount, rate,
          FusingFRLayouter.OutputFormat.valueOf(opts[3]));
        applyOptionalArguments(algo, 4);
        break;
      case "CentroidFRLayouter":
        if (opts.length < 2) {
          throw new IllegalArgumentException("Selected algorithm has 1 required arguments");
        }
        iterations = Integer.parseInt(opts[1]);
        algo = new CentroidFRLayouter(iterations, vertexcount);
        applyOptionalArguments(algo, 2);
        break;
      default:
        throw new IllegalArgumentException("Unknown algorithm: " + algoname);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Error when parsing number: " + e.getMessage());
    }
    return algo;
  }

  /**
   * Parse the optional arguments and call the according setters on the layouting-algorithm
   *
   * @param algo  The algorithm to configure
   * @param start The arguments
   */
  private static void applyOptionalArguments(LayoutingAlgorithm algo, int start) {
    for (int i = start; i < CONSTRUCTOR_PARAMS.length; i++) {
      String option = CONSTRUCTOR_PARAMS[i];
      if (!option.contains("=")) {
        throw new IllegalArgumentException("Optional arguments need to contain a '='");
      }
      String optionName = option.split("=")[0];
      String[] optionValues = option.split("=")[1].split(",");
      Object[] values = new Object[optionValues.length];
      Class[] types = new Class[optionValues.length];

      for (int o = 0; o < optionValues.length; o++) {
        try {
          values[o] = Integer.parseInt(optionValues[o]);
          types[o] = int.class;
          continue;
        } catch (Exception e) {

        }
        try {
          values[o] = Double.parseDouble(optionValues[o]);
          types[o] = double.class;
          continue;
        } catch (Exception e) {

        }
        try {
          values[o] = Boolean.parseBoolean(optionValues[o]);
          types[o] = boolean.class;
          continue;
        } catch (Exception e) {

        }
      }

      try {
        Method m = algo.getClass().getMethod(optionName, types);
        if (optionValues.length != m.getParameterCount()) {
          throw new IllegalArgumentException(
            "Wrong number of values for optional argument: " + optionName);
        }
        m.invoke(algo, values);
      } catch (NoSuchMethodException e) {
        String args = "";
        for (int z = 0; z < types.length; z++) {
          args += types[z].getName() + ",";
        }
        args = args.substring(0, args.length() - 1);
        throw new IllegalArgumentException(
          "Unknown optional argument: " + optionName + "(" + args + ")");
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Generates a folder name from the input arguments
   *
   * @return A foldername
   */
  private static String getDynamicOutputFolderName() {
    String dataset = new File(INPUT_PATH).getName();
    return dataset + "-" + String.join("-", CONSTRUCTOR_PARAMS)+"-p="+getExecutionEnvironment().getParallelism();
  }

  /**
   * Returns an EPGM DataSink for a given directory and format.
   *
   * @param directory output path
   * @param format    output format (csv, indexed, json)
   * @param config    gradoop config
   * @param alg       used algorithm
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
      int width = 1024;
      int height = 1024;
      return new Plotter(directory + "image.png", alg, width, height)
        .vertexSize(2).dynamicEdgeSize(true).dynamicVertexSize(true).edgeSize(0.1f);
    default:
      throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }

  /**
   * Reads an EPGM database from a given directory.
   *
   * @param directory path to EPGM database
   * @param format format in which the graph is stored (csv, indexed, json)
   * @return EPGM logical graph
   * @throws IOException on failure
   */
  protected static LogicalGraph readLogicalGraph(String directory, String format)
    throws IOException {
    return getDataSource(directory, format).getLogicalGraph();
  }


  /**
   * Returns an EPGM DataSource for a given directory and format.
   *
   * @param directory input path
   * @param format format in which the data is stored (csv, indexed, json)
   * @return DataSource for EPGM Data
   */
  private static DataSource getDataSource(String directory, String format) {
    directory = appendSeparator(directory);
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    format = format.toLowerCase();

    switch (format) {
    case "json":
      return new JSONDataSource(directory, config);
    case "csv":
      return new CSVDataSource(directory, config);
    case "indexed":
      return new IndexedCSVDataSource(directory, config);
    case "lgcsv":
      return new LogicalGraphCSVDataSource(directory, config);
    case "lgindexed":
      return new LogicalGraphIndexedCSVDataSource(directory, config);
    case "mtx":
      return new MtxDataSource(directory,config);
    default:
      throw new IllegalArgumentException("Unsupported format: " + format);
    }
  }



  /**
   * Method to crate and add lines to a benchmark file.
   *
   * @param result         The JoExecutionResult for the layouting
   * @param parallelism    Parallelism level used for the layouting
   * @param layouting      layouting algorithm under test
   * @param statisticValue Result of the statistic the user wanted to calculate for the created
   *                       layout
   * @throws IOException exception during file writing
   */
  private static void writeBenchmark(JobExecutionResult result, int parallelism,
    LayoutingAlgorithm layouting, double statisticValue) throws IOException {
    String head = String
      .format("%s|%s|%s|%s|%s%n", "Parallelism", "Dataset", "Params",
        "Runtime " + "[s]", "Statistic");

    // build log
    String tail = String.format("%s|%s|%s|%s|%s%n", parallelism,
      INPUT_PATH.substring(INPUT_PATH.lastIndexOf(File.separator) + 1),
      String.join(", ", layouting.toString()), result.getNetRuntime(TimeUnit.SECONDS),
      statisticValue);

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
