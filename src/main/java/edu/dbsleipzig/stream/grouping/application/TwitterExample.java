/*
 * Copyright © 2021 - 2023 Leipzig University (Database Research Group)
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
package edu.dbsleipzig.stream.grouping.application;

import edu.dbsleipzig.stream.grouping.impl.algorithm.GraphStreamGrouping;
import edu.dbsleipzig.stream.grouping.impl.algorithm.TableGroupingBase;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.AvgProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.Count;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.MaxProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.MinProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.SumProperty;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.application.functions.FilterEndpointInitializer;
import edu.dbsleipzig.stream.grouping.application.functions.TwitterMapper;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

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
        DataStream<StreamTriple> tweetStream = streamSource.flatMap(new TwitterMapper());

        // get the steam graph from the incoming socket stream via stream graph source
        StreamGraph streamGraph = StreamGraph.fromFlinkStream(tweetStream, new StreamGraphConfig(env));

        // select grouping configuration
        GraphStreamGrouping groupingOperator = new TableGroupingBase.GroupingBuilder()
          .setWindowSize(20, WindowConfig.TimeUnit.SECONDS)
          .addVertexGroupingKey(":label")
          .addVertexGroupingKey("country")
          .addVertexAggregateFunction(new Count())
          .addVertexAggregateFunction(new SumProperty("followers_count"))
          .addVertexAggregateFunction(new AvgProperty("followers_count"))
          .addVertexAggregateFunction(new MinProperty("followers_count"))
          .addVertexAggregateFunction(new MaxProperty("followers_count"))
          .addEdgeGroupingKey(":label")
          .addEdgeAggregateFunction(new Count()).build();

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
