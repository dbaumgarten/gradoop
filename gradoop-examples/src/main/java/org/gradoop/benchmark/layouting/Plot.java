package org.gradoop.benchmark.layouting;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.Plotter;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Plot {

  public static void main(String[] args) throws Exception {
    if (args.length < 5){
      System.out.println("Need at least 5 arguments");
      System.exit(1);
    }

    String input = args[0];
    int layoutWidth = Integer.parseInt(args[1]);
    int layoutHeight = Integer.parseInt(args[2]);
    int imageWidth = Integer.parseInt(args[3]);
    int imageHeight = Integer.parseInt(args[4]);

    String output = input + "/new-image.png";


    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

    CSVDataSource source = new CSVDataSource(input,cfg);
    LogicalGraph graph = source.getLogicalGraph();

    Plotter p = new Plotter(output,layoutWidth,layoutHeight,imageWidth,imageHeight);
    applyOptionalArguments(p,args,5);

    graph.writeTo(p);

    env.execute("Re-draw");
  }

  /**
   * Parse the optional arguments and call the according setters on the plotter
   *
   * @param p  The plotter to configure
   * @param start The arguments
   */
  private static void applyOptionalArguments(Plotter p, String opts[], int start) {
    for (int i = start; i < opts.length; i++) {
      String option = opts[i];
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
          values[o] = Float.parseFloat(optionValues[o]);
          types[o] = float.class;
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
        Method m = p.getClass().getMethod(optionName, types);
        if (optionValues.length != m.getParameterCount()) {
          throw new IllegalArgumentException(
            "Wrong number of values for optional argument: " + optionName);
        }
        m.invoke(p, values);
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

}
