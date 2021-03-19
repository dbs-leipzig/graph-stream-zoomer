package edu.leipzig.application;

import edu.leipzig.application.functions.FilterEndpointInitializer;
import edu.leipzig.application.functions.TwitterMapper;
import edu.leipzig.impl.algorithm.GraphStreamGrouping;
import edu.leipzig.impl.algorithm.TableGroupingBase;
import edu.leipzig.impl.functions.aggregation.AvgFreqStreamEdge;
import edu.leipzig.impl.functions.aggregation.AvgProperty;
import edu.leipzig.impl.functions.aggregation.Count;
import edu.leipzig.impl.functions.aggregation.MaxProperty;
import edu.leipzig.impl.functions.aggregation.MinProperty;
import edu.leipzig.impl.functions.aggregation.SumProperty;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Example application grouping a twitter graph stream.
 */
public class TwitterExample {

    /**
     * Execution function for the example application.
     *
     * @param args the twitter configuration
     * @throws Exception in case of an error
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Usage: TwitterExample [--output <path>] " +
          "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token " +
          "<token> --twitter-source.tokenSecret <tokenSecret>]");

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(30));

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // create the twitter source
        checkTwitterParams(params);
        final TwitterSource twitterSource = new TwitterSource(params.getProperties());
        twitterSource.setCustomEndpointInitializer(new FilterEndpointInitializer());

        // add the twitter source
        DataStream<String> streamSource = env.addSource(twitterSource);

        // create the Flink stream
        DataStream<StreamObject> tweetStream = streamSource
          .flatMap(new TwitterMapper())
          .assignTimestampsAndWatermarks(
            WatermarkStrategy
              .<StreamObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
              .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        // get the steam graph from the incoming socket stream via stream graph source
        StreamGraph streamGraph = StreamGraph.fromFlinkStream(tweetStream, new StreamGraphConfig(env, 1));

        // select grouping configuration
        GraphStreamGrouping groupingOperator = new TableGroupingBase.GroupingBuilder()
          .addVertexGroupingKey(":label")
          .addVertexGroupingKey("country")
          .addVertexAggregateFunction(new Count())
          .addVertexAggregateFunction(new SumProperty("followers_count"))
          .addVertexAggregateFunction(new AvgProperty("followers_count"))
          .addVertexAggregateFunction(new MinProperty("followers_count"))
          .addVertexAggregateFunction(new MaxProperty("followers_count"))
          .addEdgeGroupingKey(":label")
          .addEdgeAggregateFunction(new Count())
          .addEdgeAggregateFunction(new AvgFreqStreamEdge()).build();


        // execute grouping
        streamGraph = streamGraph.apply(groupingOperator);
        // OR: streamGraph = groupingOperator.execute(streamGraph);

        streamGraph.print();

        // execute program
        env.execute("Twitter Stream Grouping Example.");
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
