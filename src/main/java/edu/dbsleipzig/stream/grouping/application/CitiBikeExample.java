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
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.TupleCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Demonstration of the graph stream grouping using public citibike bike-sharing data.
 */
public class CitiBikeExample {

    public static void main(String[] args) throws Exception {
        // Init the stream environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // Create the triple stream from a csv file
        DataStream<StreamTriple> citiBikeStream = createInputFromCsv(env);

        // Init the StreamGraph - our internal representation of a graph stream
        StreamGraph streamGraph = StreamGraph.fromFlinkStream(citiBikeStream, new StreamGraphConfig(env));

        // Configure and build the grouping operator
        GraphStreamGrouping groupingOperator = new TableGroupingBase.GroupingBuilder()
          .setWindowSize(15, WindowConfig.TimeUnit.DAYS)
          .addVertexGroupingKey(":label")
          .addEdgeGroupingKey("gender")
          .addEdgeGroupingKey(":label")
          .addVertexAggregateFunction(new Count())
          .addEdgeAggregateFunction(new Count())
          .addEdgeAggregateFunction(new AvgProperty("tripduration"))
          .build();

        // Execute the grouping and overwrite the input stream with the grouping result
        streamGraph = groupingOperator.execute(streamGraph);

        // Print the result stream to console
        streamGraph.print();

        // Trigger the workflow execution
        env.execute();
    }

    public static DataStream<StreamTriple> createInputFromCsv(StreamExecutionEnvironment env) {

        TupleTypeInfo<
          Tuple15<String, String, String, String, String, String, String, String, String,
            String, String, String, String, String, String>> tuple15TupleTypeInfo =
                TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(
                  String.class, String.class, String.class, String.class, String.class,
                  String.class, String.class, String.class, String.class, String.class,
                  String.class, String.class, String.class, String.class, String.class);


        CsvInputFormat<
          Tuple15<
            String, String, String, String, String, String, String, String,
            String, String, String, String, String, String, String>> inputFormat =
          // todo: use path from Resources
          new TupleCsvInputFormat<>(new Path("./src/main/resources/citibike-data/201306-citibike-tripdata.csv"), tuple15TupleTypeInfo);
        inputFormat.setSkipFirstLineAsHeader(true);
        DataStreamSource<Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>>
                source = env.createInput(inputFormat, tuple15TupleTypeInfo);

        // Todo: outsource this map a separate class
        DataStream<StreamTriple> tripleDataStream = source.map((MapFunction<Tuple15<String, String, String, String, String, String, String, String, String, String, String, String, String, String, String>, StreamTriple>) tuple15 -> {
            StreamTriple streamTriple = new StreamTriple();
            StreamVertex sourceVertex = new StreamVertex();
            StreamVertex targetVertex = new StreamVertex();

            HashMap<String, Object> edgePropMap = new HashMap<>();
            HashMap<String, Object> sourcePropMap = new HashMap<>();
            HashMap<String, Object> targetPropMap = new HashMap<>();

            edgePropMap.put("tripduration", Integer.parseInt(tuple15.f0));
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date parseStopTime = simpleDateFormat.parse(tuple15.f2.replaceAll("\"", ""));
            Timestamp stopTime = new Timestamp(parseStopTime.getTime());
            edgePropMap.put("starttime", tuple15.f1);
            edgePropMap.put("stoptime", tuple15.f2);
            streamTriple.setTimestamp(stopTime);
            sourceVertex.setEventTime(stopTime);
            targetVertex.setEventTime(stopTime);
            sourceVertex.setVertexId(tuple15.f3);
            sourceVertex.setVertexLabel(tuple15.f4);
            sourcePropMap.put("lat", Double.parseDouble(tuple15.f5));
            sourcePropMap.put("long", Double.parseDouble(tuple15.f6));
            targetVertex.setVertexId(tuple15.f7);
            targetVertex.setVertexLabel(tuple15.f8);
            if (!tuple15.f9.equalsIgnoreCase("NULL")){
                targetPropMap.put("lat", Double.parseDouble(tuple15.f9));
            }
            if (!tuple15.f10.equalsIgnoreCase("NULL")) {
                targetPropMap.put("long", Double.parseDouble(tuple15.f10));
            }
            streamTriple.setId(tuple15.f11);
            streamTriple.setLabel(tuple15.f12);
            if (tuple15.f13 != null && !tuple15.f13.equals("NULL")) {
                edgePropMap.put("birth year", Integer.parseInt(tuple15.f13));
            }
            edgePropMap.put("gender", Integer.parseInt(tuple15.f14));;
            sourceVertex.setVertexProperties(Properties.createFromMap(sourcePropMap));
            targetVertex.setVertexProperties(Properties.createFromMap(targetPropMap));
            streamTriple.setSource(sourceVertex);
            streamTriple.setTarget(targetVertex);
            streamTriple.setProperties(Properties.createFromMap(edgePropMap));

            return streamTriple;
        });
        return tripleDataStream;
    }
}
