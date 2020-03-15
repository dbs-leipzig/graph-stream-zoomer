package edu.leipzig.example;

import edu.leipzig.example.functions.TwitterMapper;
import edu.leipzig.impl.functions.aggregation.Count;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.model.streamGraph.StreamGraph;
import edu.leipzig.model.streamGraph.StreamGraphConfig;
import edu.leipzig.model.streamGraph.StreamGraphSource;
import edu.leipzig.model.streamGraph.StreamObject;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.ArrayList;
import java.util.List;


public class TwitterImporter {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: TwitterExample [--output <path>] " +
                "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token " +
                "<token> --twitter-source.tokenSecret <tokenSecret>]");

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> streamSource;
        if (params.has(TwitterSource.CONSUMER_KEY) && params.has(TwitterSource.CONSUMER_SECRET) &&
                params.has(TwitterSource.TOKEN) && params.has(TwitterSource.TOKEN_SECRET)) {
            streamSource = env.addSource(new TwitterSource(params.getProperties()));
        } else {
            System.out.println("Executing TwitterStream example with default props.");
            System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                    "--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the " +
                    "authentication info.");
            return;
        }

        DataStream<StreamObject> tweetStream = streamSource.flatMap(new TwitterMapper())
                .assignTimestampsAndWatermarks(new StreamingJob.myTimestampsAndWatermarksAssigner());

        /*
         create stream graph environment configuration with query retention interval to prevent excessive state size
         here for example, set idle state retention time: min = 1 hours, max = 2 hours
        * */
        StreamGraphConfig streamGraphConfig = new StreamGraphConfig(env, 1, 2);

        // select grouping configuration
        List<String> vertexGroupingKeys = new ArrayList<>();
        List<CustomizedAggregationFunction> vertexAggregationFunctions = new ArrayList<>();
        List<String> edgeGroupingKeys = new ArrayList<>();
        List<CustomizedAggregationFunction> edgeAggregationFunctions = new ArrayList<>();
        vertexGroupingKeys.add(":label");       // vertex label grouping
        vertexAggregationFunctions.add(new Count());
        edgeGroupingKeys.add(":label");          // edge label grouping
        edgeGroupingKeys.add("timestamp_10.sec"); // edge grouping on timestamp (per sec, 10.sec, min, 10.min, h, d, m, y)
        edgeAggregationFunctions.add(new Count());

        // get the steam graph from the incoming socket stream via stream graph source
        StreamGraph streamGraph = new StreamGraphSource(tweetStream).getStreamGraph(streamGraphConfig);
        // execute grouping
        streamGraph.groupBy(vertexGroupingKeys, vertexAggregationFunctions, edgeGroupingKeys, edgeAggregationFunctions).writeTo();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

}
