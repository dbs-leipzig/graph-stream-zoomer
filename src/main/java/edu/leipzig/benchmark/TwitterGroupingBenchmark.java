package edu.leipzig.benchmark;

import edu.leipzig.application.functions.TwitterMapper;
import edu.leipzig.impl.functions.aggregation.AvgFreqStreamEdge;
import edu.leipzig.impl.functions.aggregation.Count;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.impl.functions.aggregation.MaxProperty;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for twitter stream.
 */
public class TwitterGroupingBenchmark {

    /**
     * Main program to run the benchmark. Arguments are the available options.
     *
     * @param args program arguments
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Usage: TwitterExample [--output <path>] " +
          "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token " +
          "<token> --twitter-source.tokenSecret <tokenSecret>]");

        // set up the streaming execution environment
        // time characteristic is EVENT time by default
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(5), CheckpointingMode.AT_LEAST_ONCE);

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        checkTwitterParams(params);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> streamSource = env.addSource(new TwitterSource(params.getProperties()));

        DataStream<StreamObject> tweetStream = streamSource.flatMap(new TwitterMapper()).assignTimestampsAndWatermarks(
          WatermarkStrategy
            .<StreamObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        /*
         * create stream graph environment configuration with query retention interval to prevent excessive
         * state size - here for example, set idle state retention time: min = 12 hours, max = 1.5 * min
         */
        StreamGraphConfig streamGraphConfig = new StreamGraphConfig(env, 12);

        // get the steam graph from the incoming socket stream via stream graph source
        StreamGraph streamGraph = StreamGraph.fromFlinkStream(tweetStream, streamGraphConfig);

        // select grouping configuration
        List<String> vertexGroupingKeys = new ArrayList<>();
        List<CustomizedAggregationFunction> vertexAggregationFunctions = new ArrayList<>();
        List<String> edgeGroupingKeys = new ArrayList<>();
        List<CustomizedAggregationFunction> edgeAggregationFunctions = new ArrayList<>();
        vertexGroupingKeys.add(":label");       // vertex label grouping
        vertexAggregationFunctions.add(new Count());
        vertexAggregationFunctions.add(new MaxProperty("friends_count"));
        edgeGroupingKeys.add(":label");          // edge label grouping
        edgeAggregationFunctions.add(new Count());
        edgeAggregationFunctions.add(new AvgFreqStreamEdge());

        // execute grouping
        streamGraph
          .groupBy(vertexGroupingKeys, vertexAggregationFunctions, edgeGroupingKeys, edgeAggregationFunctions)
          .print();

        // execute program
        env.execute("Twitter Grouping");
    }

    private static void checkTwitterParams(ParameterTool params) {
        if (!params.has(TwitterSource.CONSUMER_KEY) || !params.has(TwitterSource.CONSUMER_SECRET) ||
          !params.has(TwitterSource.TOKEN) || !params.has(TwitterSource.TOKEN_SECRET)) {
            System.out.println("Executing TwitterStream example with default props.");
            System.out.println(
              "Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                "--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the " +
                "authentication info.");
            System.exit(0);
        }
    }
}
