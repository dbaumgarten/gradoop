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
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexSampling;
import org.gradoop.flink.model.impl.operators.statistics.CrossEdges;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/** Benchmark for the graph-layouting
 *
 */
public class GraphScaler extends AbstractRunner implements ProgramDescription {

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
   * List of parameters that are used to instantiate the selected layouting algorithm.
   */
  private static String[] CONSTRUCTOR_PARAMS;
  /** Output-format choosen by user
   *
   */
  private static String OUTPUT_FORMAT;


  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true,
      "Path to directory containing csv files to be processed");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to directory where resulting graph sample, benchmark file and graph " +
        "statistics are written to. (Defaults to " + OUTPUT_PATH_DEFAULT + ")");
    OPTIONS.addOption(OPTION_INPUT_FORMAT, "format", true,
      "Format of the input data. Defaults to 'csv'");
    OPTIONS.addOption(OPTION_OUTPUT_FORMAT,"outformat", true,"Select output format");
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

    graph = graph.sample(new RandomVertexSampling(Float.parseFloat(CONSTRUCTOR_PARAMS[0])));

    graph.writeTo(getDataSink(OUTPUT_PATH,OUTPUT_FORMAT,graph.getConfig()),true);

    getExecutionEnvironment().execute("Sampling");

  }

  /**
   * Returns an EPGM DataSink for a given directory and format.
   *
   * @param directory output path
   * @param format output format (csv, indexed, json)
   * @param config gradoop config
   * @return DataSink for EPGM Data
   */
  private static DataSink getDataSink(String directory, String format, GradoopFlinkConfig config) {
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
      return new Plotter(new Plotter.Options().dimensions(width,height).ignoreVertices(true).scaleImage(1000,1000),directory+"image.png");
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
    CONSTRUCTOR_PARAMS = cmd.getArgList().toArray(new String[0]);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH, OUTPUT_PATH_DEFAULT);
    INPUT_FORMAT = cmd.getOptionValue(OPTION_INPUT_FORMAT, INPUT_FORMAT_DEFAULT);
    OUTPUT_FORMAT = cmd.getOptionValue(OPTION_OUTPUT_FORMAT);
  }

  @Override
  public String getDescription() {
    return this.getClass().getName();
  }
}
