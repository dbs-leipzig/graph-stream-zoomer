package edu.leipzig.benchmark;

import edu.leipzig.application.functions.JSONToStreamObjectMapper;
import edu.leipzig.impl.functions.aggregation.AvgFreqStreamEdge;
import edu.leipzig.impl.functions.aggregation.AvgProperty;
import edu.leipzig.impl.functions.aggregation.Count;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark for synthetic stream.
 */
public class SyntheticStreamGrouping {

  /**
   * Output file path
   */
  private static String OUTPUT_FILE;
  /**
   * Used grouping configuration
   */
  private static int GROUPING_CONFIG;

  /**
   * True if graph layout should be used
   */
  private static boolean USE_GRAPH_LAYOUT;

  /**
   * min query retention interval
   */
  private static int MIN_RETENTION_TIME;

  /**
   * the file name of the data set
   */
  private static String DATA_SET_FILE;

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    GROUPING_CONFIG = Integer.parseInt(args[0]);
    USE_GRAPH_LAYOUT = Boolean.parseBoolean(args[1]);
    MIN_RETENTION_TIME = Integer.parseInt(args[2]);
    DATA_SET_FILE = args[4];
    OUTPUT_FILE = args[5];
    System.out.printf("Running using params:\n grouping config: %d, graph layout: %b," +
        " min query retention interval: %d hours%n", GROUPING_CONFIG,
      USE_GRAPH_LAYOUT, MIN_RETENTION_TIME);

    runBenchmark();
  }

  /**
   * Main method to run the grouping algorithm.
   */
  private static void runBenchmark() throws Exception {
    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //connects to a socket source stream
    DataStream<StreamTriple> socketStream =
      env.readTextFile(DATA_SET_FILE) //.socketTextStream("localhost", 6661) //
        .map(new JSONToStreamObjectMapper())
        // extracts the event timestamp from each record in order to create watermarks to signal event time
        // progress.
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
            .<StreamTriple>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()));

    StreamGraphConfig streamGraphConfig = new StreamGraphConfig(env);
    streamGraphConfig.setAndApplyIdleStateRetentionTime(Duration.ofHours(MIN_RETENTION_TIME));
    // get the stream graph from the incoming socket stream via stream graph source
    StreamGraph streamGraph = StreamGraph.fromFlinkStream(socketStream, streamGraphConfig);

    List<String> vertexGroupingKeys = new ArrayList<>();
    List<CustomizedAggregationFunction> vertexAggregationFunctions = new ArrayList<>();
    List<String> edgeGroupingKeys = new ArrayList<>();
    List<CustomizedAggregationFunction> edgeAggregationFunctions = new ArrayList<>();
    switch (GROUPING_CONFIG) {
    case 1:
      // basic grouping
      vertexGroupingKeys.add(":label");
      vertexAggregationFunctions.add(new Count());
      edgeGroupingKeys.add(":label");
      edgeAggregationFunctions.add(new Count());
      break;
    case 2:
      // grouping on vertex property plus the avg agg func
      vertexGroupingKeys.add("city");
      vertexAggregationFunctions.add(new Count());
      vertexAggregationFunctions.add(new AvgProperty("salary", "avg_salary"));
      edgeGroupingKeys.add(":label");
      edgeAggregationFunctions.add(new Count());
      break;
    case 3:
      // grouping on edge property
      vertexGroupingKeys.add(":label");
      vertexAggregationFunctions.add(new Count());
      edgeGroupingKeys.add("timestamp");
      edgeAggregationFunctions.add(new Count());
      break;
    case 4:
      // grouping using the freq agg func
      vertexGroupingKeys.add(":label");
      edgeGroupingKeys.add(":label");
      edgeAggregationFunctions.add(new AvgFreqStreamEdge());
      break;
    default:
      throw new RuntimeException("Unsupported grouping configuration!");
    }

    streamGraph = streamGraph
      .groupBy(vertexGroupingKeys, vertexAggregationFunctions, edgeGroupingKeys, edgeAggregationFunctions);
    if (USE_GRAPH_LAYOUT) {
      // execute graph grouping
      streamGraph.writeGraphAsCsv(DATA_SET_FILE);
    } else {
      // execute grouping of vertices and edges
      streamGraph
        .writeAsCsv(DATA_SET_FILE);
    }

    // execute program
    final JobExecutionResult jobResult = env.execute("Stream Grouping");

    /*
     * Write statistics about the job result
     */
    long runtimeInMs = jobResult.getNetRuntime();
    int parallelism = env.getParallelism();
    String logMessage = String.format("%s, %s, %s", runtimeInMs, parallelism, DATA_SET_FILE);

    try {
      FileOutputStream csvInfo = new FileOutputStream(OUTPUT_FILE, true);
      DataOutputStream csvOutStream = new DataOutputStream(new BufferedOutputStream(csvInfo));
      csvOutStream.writeUTF(logMessage + "\n");
      csvOutStream.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.out.printf("runtimeInMs: %d, parallelism: %d, data set file: %s%n",
      runtimeInMs,
      parallelism,
      DATA_SET_FILE);
  }
}
