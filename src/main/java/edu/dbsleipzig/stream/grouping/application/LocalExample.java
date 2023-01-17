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

import edu.dbsleipzig.stream.grouping.impl.algorithm.TableGroupingBase;
import edu.dbsleipzig.stream.grouping.impl.functions.aggregation.Count;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.util.HashMap;

/**
 * A local example with some test data graph stream.
 */
public class LocalExample {

    /**
     * The executable main function.
     *
     * @param args cmd args not needed
     * @throws Exception in case of an error
     */
    public static void main(String[] args) throws Exception {

        // Increase number of network buffers to prevent IOException
        Configuration cfg = new Configuration();
        cfg.setString("taskmanager.memory.network.max", "1gb");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(cfg);

        DataStream<StreamTriple> graphStreamTriples = createStreamTriples(env);

        StreamGraph streamGraph = StreamGraph.fromFlinkStream(graphStreamTriples, new StreamGraphConfig(env));

        TableGroupingBase.GroupingBuilder groupingBuilder = new TableGroupingBase.GroupingBuilder();

        // Group edges and vertices on 'label'-property and count the amount.
        groupingBuilder.addVertexGroupingKey(":label");
        groupingBuilder.addEdgeGroupingKey(":label");
        groupingBuilder.addVertexAggregateFunction(new Count());
        groupingBuilder.addEdgeAggregateFunction(new Count());

        streamGraph = groupingBuilder.build().execute(streamGraph);

        streamGraph.print();

        env.execute();
    }


    /**
     * Creates custom StreamTriples.
     *
     * @param env StreamTableEnvironment of the StreamGraph
     * @return StreamTriples derived from custom edges and vertices.
     */
    public static DataStream<StreamTriple> createStreamTriples(StreamExecutionEnvironment env) {

        /*
        t(i+1) = t(i) + 10s
         */
        Timestamp t1 = new Timestamp(1619511661000L);
        Timestamp t2 = new Timestamp(1619511662000L);
        Timestamp t3 = new Timestamp(1619511673000L);
        Timestamp t4 = new Timestamp(1619511674000L);

        /*
        Create vertices with empty properties
         */
        StreamVertex v1 = new StreamVertex("v1", "A", Properties.create(), t1);
        StreamVertex v2 = new StreamVertex("v2", "B", Properties.create(), t1);
        StreamVertex v3 = new StreamVertex("v3", "A", Properties.create(), t2);
        StreamVertex v4 = new StreamVertex("v4", "B", Properties.create(), t2);
        StreamVertex v5 = new StreamVertex("v5", "A", Properties.create(), t3);
        StreamVertex v6 = new StreamVertex("v6", "B", Properties.create(), t3);
        StreamVertex v7 = new StreamVertex("v7", "A", Properties.create(), t4);
        StreamVertex v8 = new StreamVertex("v8", "B", Properties.create(), t4);

        /*
        Define custom vertex properties
         */
        HashMap<String, Object> propertiesVertexV1 = new HashMap<>();
        propertiesVertexV1.put("Relevance", 1);
        propertiesVertexV1.put("Size", 15);
        propertiesVertexV1.put("Weekday", "Monday");
        Properties propertiesV1 = Properties.createFromMap(propertiesVertexV1);

        HashMap<String, Object> propertiesVertexV2 = new HashMap<>();
        propertiesVertexV2.put("Relevance", 3);
        propertiesVertexV2.put("Size", 10);
        //propertiesVertexV2.put("Weekday", "Tuesday");
        Properties propertiesV2 = Properties.createFromMap(propertiesVertexV2);

        HashMap<String,Object> propertiesWithoutSize = new HashMap<>();
        propertiesWithoutSize.put("Relevance", 2);
        propertiesWithoutSize.put("Weekday","Monday");
        Properties propertiesCustom = Properties.createFromMap(propertiesWithoutSize);

        HashMap<String, Object> propertiesVertexV3 = new HashMap<>();
        propertiesVertexV3.put("Relevance", 2);
        propertiesVertexV3.put("Size", 30);
        propertiesVertexV3.put("Weekday", "Monday");
        Properties propertiesV3 = Properties.createFromMap(propertiesVertexV3);

        HashMap<String, Object> propertiesVertexV4 = new HashMap<>();
        propertiesVertexV4.put("Relevance", 5);
        propertiesVertexV4.put("Size", 5);
        propertiesVertexV4.put("Weekday", "Thursday");
        Properties propertiesV4 = Properties.createFromMap(propertiesVertexV4);

        /*
        Assign vertex properties
         */
        v1.setVertexProperties(propertiesV1);
        v2.setVertexProperties(propertiesV2);
        v3.setVertexProperties(propertiesV3);
        v4.setVertexProperties(propertiesV4);
        v5.setVertexProperties(propertiesCustom);
        v6.setVertexProperties(propertiesV2);
        v7.setVertexProperties(propertiesV3);
        v8.setVertexProperties(propertiesV4);

        /*
        Create custom edge properties
         */
        HashMap<String, Object> propertiesEdge1 = new HashMap<>();
        propertiesEdge1.put("Weekday", "Thursday");
        Properties propertiesE1 = Properties.createFromMap(propertiesEdge1);

        HashMap<String, Object> propertiesEdge2 = new HashMap<>();
        propertiesEdge2.put("Weight", 6);
        Properties propertiesE2 = Properties.createFromMap(propertiesEdge2);

        HashMap<String,Object> propertiesEdge3 = new HashMap<>();
        propertiesEdge3.put("Weekday", "Thursday");
        propertiesEdge3.put("Weight", 3);
        Properties propertiesE3 = Properties.createFromMap(propertiesEdge3);

        StreamTriple edge1 = new StreamTriple("e1", t1, "impacts",  propertiesE1, v1, v2);
        StreamTriple edge2 = new StreamTriple("e2", t1, "impacts", propertiesE2, v3, v4);
        StreamTriple edge3 = new StreamTriple("e3", t2, "calculates", propertiesE3, v3, v4);
        StreamTriple edge4 = new StreamTriple("e4", t2, "impacts",  propertiesE1, v1, v2);
        StreamTriple edge5 = new StreamTriple("e5", t3, "impacts", propertiesE2, v5, v6);
        StreamTriple edge6 = new StreamTriple("e6", t3, "calculates", propertiesE3, v5, v6);
        StreamTriple edge7 = new StreamTriple("e7", t4, "impacts",  propertiesE1, v7, v8);
        StreamTriple edge8 = new StreamTriple("e8", t4, "impacts", propertiesE2, v7, v8);
        StreamTriple edge9 = new StreamTriple("e9", t1, "calculates", propertiesE3, v7, v8);

        DataStream<StreamTriple> graphStreamTriples = env.fromElements(edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8, edge9);

        return graphStreamTriples;
    }
}
