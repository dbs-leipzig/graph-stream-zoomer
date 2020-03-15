/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.leipzig.example;

import edu.leipzig.example.functions.JSONToStreamObjectMapper;
import edu.leipzig.impl.functions.aggregation.Count;
import edu.leipzig.impl.functions.aggregation.CustomizedAggregationFunction;
import edu.leipzig.model.streamGraph.StreamGraph;
import edu.leipzig.model.streamGraph.StreamGraphConfig;
import edu.leipzig.model.streamGraph.StreamGraphSource;
import edu.leipzig.model.streamGraph.StreamObject;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

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
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         /*
            setting the time characteristic as Event Time which is embedded within the records before they enter Flink.
        * */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		 /*
        connects to a socket source stream
        * */
        DataStream<StreamObject> socketStream = env.socketTextStream("localhost", 6666)
                .map(new JSONToStreamObjectMapper())
                // extracts the event timestamp from each record in order to create watermarks to signal event time progress.
                .assignTimestampsAndWatermarks(new myTimestampsAndWatermarksAssigner());
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
        // vertexGroupingKeys.add("domain");    // vertex grouping on property field in Project class
        // vertexGroupingKeys.add("city");      // vertex grouping on property field in Company class
        // vertexGroupingKeys.add("nothing");   // Testing vertex grouping on not existed property
        vertexAggregationFunctions.add(new Count());
        // vertexAggregationFunctions.add(new AvgProperty("salary", "avg_salary"));    // Testing Avg agg func on vertex property field in Developer class
        // vertexAggregationFunctions.add(new SumProperty("salary", "sum_salary"));     // Testing Sum agg func on vertex property field in Developer class
        // vertexAggregationFunctions.add(new MaxProperty("salary", "max_salary"));     // Testing Max agg func on vertex property field in Developer class
        edgeGroupingKeys.add(":label");          // edge label grouping
        // edgeGroupingKeys.add("month");       // edge grouping on property field in Action class
        edgeGroupingKeys.add("timestamp_10.sec"); // edge grouping on timestamp (per sec, 10.sec, min, 10.min, h, d, m, y)
        edgeAggregationFunctions.add(new Count());
        // edgeAggregationFunctions.add(new AvgFreqStreamEdge());

        // get the stream graph from the incoming socket stream via stream graph source
        StreamGraph streamGraph = new StreamGraphSource(socketStream).getStreamGraph(streamGraphConfig);
        // execute grouping
        // streamGraph.groupBy(vertexGroupingKeys).writeTo();
        // streamGraph.groupBy(vertexGroupingKeys, edgeGroupingKeys).writeTo();
        streamGraph.groupBy(vertexGroupingKeys, vertexAggregationFunctions, edgeGroupingKeys, edgeAggregationFunctions).writeTo();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class myTimestampsAndWatermarksAssigner extends AscendingTimestampExtractor<StreamObject> {

        @Override
        public long extractAscendingTimestamp(StreamObject element) {
            return element.getTimestamp();
        }
    }
}
