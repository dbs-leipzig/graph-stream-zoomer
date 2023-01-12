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

import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.Count;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.application.functions.JSONToStreamObjectMapper;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class SocketExample {

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // connects to a socket source stream
    DataStream<StreamTriple> socketStream =
      env.socketTextStream("localhost", 6666).map(new JSONToStreamObjectMapper())
        // extracts the event timestamp from each record in order to create watermarks to signal event time
        // progress.
        .assignTimestampsAndWatermarks(
          WatermarkStrategy.<StreamTriple>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime()));

    // get the stream graph from the incoming socket stream via stream graph source
    StreamGraph streamGraph = StreamGraph.fromFlinkStream(socketStream, new StreamGraphConfig(env));

    // select grouping configuration
    List<String> vertexGroupingKeys = new ArrayList<>();
    List<CustomizedAggregationFunction> vertexAggregationFunctions = new ArrayList<>();
    List<String> edgeGroupingKeys = new ArrayList<>();
    List<CustomizedAggregationFunction> edgeAggregationFunctions = new ArrayList<>();
    vertexGroupingKeys.add(":label");       // vertex label grouping
    // vertexGroupingKeys.add("domain");    // vertex grouping on property field in Project class
    // vertexGroupingKeys.add("city");      // vertex grouping on property field in Company class
    // vertexGroupingKeys.add("nothing");   // Testing vertex grouping on not existed property
    vertexAggregationFunctions.add(new Count());
    // vertexAggregationFunctions.add(new AvgProperty("salary", "avg_salary"));    // Testing Avg agg func
    // on vertex property field in Developer class
    // vertexAggregationFunctions.add(new SumProperty("salary", "sum_salary"));     // Testing Sum agg func
    // on vertex property field in Developer class
    // vertexAggregationFunctions.add(new MaxProperty("salary", "max_salary"));     // Testing Max agg func
    // on vertex property field in Developer class
    edgeGroupingKeys.add(":label");          // edge label grouping
    // edgeGroupingKeys.add("month");       // edge grouping on property field in Action class
    edgeGroupingKeys
      .add("timestamp_10.sec"); // edge grouping on timestamp (per sec, 10.sec, min, 10.min, h, d, m, y)
    edgeAggregationFunctions.add(new Count());
    // edgeAggregationFunctions.add(new AvgFreqStreamEdge());

    // execute grouping
    streamGraph
      .groupBy(vertexGroupingKeys, vertexAggregationFunctions, edgeGroupingKeys, edgeAggregationFunctions)
      .print();

    // execute program
    env.execute("Graph grouping example.");
  }

}
