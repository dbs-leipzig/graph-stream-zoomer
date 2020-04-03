package edu.leipzig.benchmark;

import edu.leipzig.example.StreamingJob;
import edu.leipzig.example.functions.JSONToStreamObjectMapper;
import edu.leipzig.impl.functions.aggregation.*;
import edu.leipzig.model.streamGraph.StreamGraph;
import edu.leipzig.model.streamGraph.StreamGraphConfig;
import edu.leipzig.model.streamGraph.StreamGraphSource;
import edu.leipzig.model.streamGraph.StreamObject;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class SyntheticStreamGrouping {

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
    private static long MIN_RETENTION_TIME;

    /**
     * max query retention interval
     */
    private static long MAX_RETENTION_TIME;

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
        MIN_RETENTION_TIME = Long.parseLong(args[2]);
        MAX_RETENTION_TIME = Long.parseLong(args[3]);
        DATA_SET_FILE = args[4];
        OUTPUT_FILE = args[5];
        System.out.println(String.format("Running using params:\n grouping config: %d, graph layout: %b," +
                        " min query retention interval: %d hours, max query retention interval: %d hours",
                GROUPING_CONFIG, USE_GRAPH_LAYOUT, MIN_RETENTION_TIME, MAX_RETENTION_TIME));

        Summarize();
    }

    /**
     * Main method to run the grouping algorithm.
     */
    private static void Summarize() throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         /*
            setting the time characteristic as Event Time which is embedded within the records before they enter Flink.
        * */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		 /*
        connects to a socket source stream
        * */
        DataStream<StreamObject> socketStream = env.readTextFile(DATA_SET_FILE) //.socketTextStream("localhost", 6661) //
                .map(new JSONToStreamObjectMapper())
                // extracts the event timestamp from each record in order to create watermarks to signal event time progress.
                .assignTimestampsAndWatermarks(new StreamingJob.myTimestampsAndWatermarksAssigner());
        /*
         create stream graph environment configuration with query retention interval to prevent excessive state size
        * */
        StreamGraphConfig streamGraphConfig = new StreamGraphConfig(env, MIN_RETENTION_TIME, MAX_RETENTION_TIME);
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

        // get the stream graph from the incoming socket stream via stream graph source
        StreamGraph streamGraph = new StreamGraphSource(socketStream).getStreamGraph(streamGraphConfig);

        if (USE_GRAPH_LAYOUT) {
            // execute graph grouping
            streamGraph.groupBy(vertexGroupingKeys, vertexAggregationFunctions,
                    edgeGroupingKeys, edgeAggregationFunctions).writeGraphAsCsv(DATA_SET_FILE);
        } else {
            // execute grouping of vertices and edges
            streamGraph.groupBy(vertexGroupingKeys, vertexAggregationFunctions,
                    edgeGroupingKeys, edgeAggregationFunctions).writeAsCsv(DATA_SET_FILE);
        }

        // execute program
        final JobExecutionResult jobResult = env.execute("Stream Grouping");



        long runtimeInMs = jobResult.getNetRuntime();
        int parallelism = env.getParallelism();
        String logMessage = String.format("%s, %s, %s",
                runtimeInMs, parallelism, DATA_SET_FILE);

        try {
            FileOutputStream csvInfo = new FileOutputStream(OUTPUT_FILE, true);
            DataOutputStream csvOutStream = new DataOutputStream(new BufferedOutputStream(csvInfo));
            csvOutStream.writeUTF(logMessage + "\n");
            csvOutStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(String.format("runtimeInMs: %d, parallelism: %d, data set file: %s",
                runtimeInMs, parallelism, DATA_SET_FILE));
    }
}
