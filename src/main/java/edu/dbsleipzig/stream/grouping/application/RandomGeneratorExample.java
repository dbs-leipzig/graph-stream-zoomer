/*
 * Copyright © 2021 - 2024 Leipzig University (Database Research Group)
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

import edu.dbsleipzig.stream.grouping.application.functions.RandomStreamTripleGenerator;
import edu.dbsleipzig.stream.grouping.impl.algorithm.GraphStreamGrouping;
import edu.dbsleipzig.stream.grouping.impl.algorithm.TableGroupingBase;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.Count;
import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.util.HashMap;

/**
 * A local example with some randomly generated graph stream.
 *
 * Use it like {@code ./bin/flink run -c edu.dbsleipzig.stream.grouping.application.RandomGeneratorExample
 * graph-stream-grouping-0.1-SNAPSHOT.jar 10 1000} for a window size of 10 seconds and
 * 1000 elements per second.
 */
public class RandomGeneratorExample {

    /**
     * The executable main function.
     *
     * @param args cmd args not needed
     * @throws Exception in case of an error
     */
    public static void main(String[] args) throws Exception {
        // Get the window size from args[0]
        int windowSize = Integer.parseInt(args[0]);

        // Get the number of elements per sec args[1]
        int elementsPerSecond = Integer.parseInt(args[1]);

        // Init the Stream Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Init the source
        DataGeneratorSource<StreamTriple> source = new DataGeneratorSource<>(
          new RandomStreamTripleGenerator(10, 10, 1000), elementsPerSecond, 1000000L);

        // Init the artificial random data stream
        DataStream<StreamTriple> graphStreamTriples =
          env.addSource(source).returns(TypeInformation.of(StreamTriple.class));

        // Map the stream to a stream graph
        StreamGraph streamGraph = StreamGraph.fromFlinkStream(graphStreamTriples, new StreamGraphConfig(env));

        // Init the grouping operator
        GraphStreamGrouping groupingOperator = new TableGroupingBase.GroupingBuilder()
          // Use 10 seconds window
          .setWindowSize(windowSize, WindowConfig.TimeUnit.SECONDS)
          // Group edges and vertices on 'label'
          .addVertexGroupingKey(":label")
          .addEdgeGroupingKey(":label")
          // Count elements
          .addVertexAggregateFunction(new Count())
          .addEdgeAggregateFunction(new Count())
          .build();

        // Apply the grouping operator
        streamGraph = groupingOperator.execute(streamGraph);

        // Print the resulting triples
        streamGraph.print();

        // Or print vertices/edges separately
        //streamGraph.printVertices();
        //streamGraph.printEdges();

        // Execute the Flink Workflow (mandatory)
        env.execute("GSZ - Random (Window: " + windowSize + " sec," + elementsPerSecond + " elem./s)");
    }
}
