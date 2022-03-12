package edu.leipzig.application;

import edu.leipzig.application.functions.JSONToStreamObjectMapper;
import edu.leipzig.impl.algorithm.TableGroupingBase;
import edu.leipzig.impl.functions.aggregation.*;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

public class GraphSummarizationJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Timestamp t1 = new Timestamp(1619511661000L);
        Timestamp t2 = new Timestamp(1619511662000L);
        Timestamp t3 = new Timestamp(1619511673000L);
        Timestamp t4 = new Timestamp(1619511674000L);
        StreamVertex v1 = new StreamVertex("v1", "A", Properties.create(), t1);
        StreamVertex v2 = new StreamVertex("v2", "B", Properties.create(), t1);
        StreamVertex v3 = new StreamVertex("v3", "A", Properties.create(), t2);
        StreamVertex v4 = new StreamVertex("v4", "B", Properties.create(), t2);
        StreamVertex v5 = new StreamVertex("v5", "A", Properties.create(), t3);
        StreamVertex v6 = new StreamVertex("v6", "B", Properties.create(), t3);
        StreamVertex v7 = new StreamVertex("v7", "A", Properties.create(), t4);
        StreamVertex v8 = new StreamVertex("v8", "B", Properties.create(), t4);

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

        v1.setVertexProperties(propertiesV1);
        v2.setVertexProperties(propertiesV2);
        v3.setVertexProperties(propertiesV3);
        v4.setVertexProperties(propertiesV4);
        v5.setVertexProperties(propertiesV1);
        v6.setVertexProperties(propertiesV2);
        v7.setVertexProperties(propertiesV3);
        v8.setVertexProperties(propertiesV4);

        HashMap<String, Object> propertiesEdge1 = new HashMap<>();
        propertiesEdge1.put("Weight", 5);
        propertiesEdge1.put("Weekday", "Thursday");
        Properties propertiesE1 = Properties.createFromMap(propertiesEdge1);

        HashMap<String, Object> propertiesEdge2 = new HashMap<>();
        propertiesEdge2.put("Weight", 6);
        //propertiesEdge2.put("Weekday", "Wednesday");
        Properties propertiesE2 = Properties.createFromMap(propertiesEdge2);

        StreamTriple edge1 = new StreamTriple("e1", t1, "impacts",  propertiesE1, v1, v2);
        StreamTriple edge2 = new StreamTriple("e2", t2, "impacts", propertiesE2, v3, v4);
        StreamTriple edge3 = new StreamTriple("e3", t2, "calculates", propertiesE1, v3, v4);
        StreamTriple edge4 = new StreamTriple("e4", t1, "impacts",  propertiesE1, v1, v2);
        StreamTriple edge5 = new StreamTriple("e5", t3, "impacts", propertiesE2, v5, v6);
        StreamTriple edge6 = new StreamTriple("e6", t3, "calculates", propertiesE1, v5, v6);
        StreamTriple edge7 = new StreamTriple("e7", t4, "impacts",  propertiesE1, v7, v8);
        StreamTriple edge8 = new StreamTriple("e8", t4, "impacts", propertiesE2, v7, v8);
        StreamTriple edge9 = new StreamTriple("e9", t4, "calculates", propertiesE1, v7, v8);

        DataStream<StreamTriple> testStream = env.fromElements(edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8, edge9);

        StreamGraph streamGraph = StreamGraph.fromFlinkStream(testStream, new StreamGraphConfig(env));

        TableGroupingBase.GroupingBuilder groupingBuilder = new TableGroupingBase.GroupingBuilder();

        groupingBuilder.addVertexGroupingKey("Weekday");
        groupingBuilder.addVertexGroupingKey(":label");
        //groupingBuilder.addEdgeGroupingKey(":label");
        groupingBuilder.addVertexAggregateFunction(new Count());
        groupingBuilder.addEdgeAggregateFunction(new Count());
        groupingBuilder.addEdgeGroupingKey("Weekday");

        streamGraph = groupingBuilder.build().execute(streamGraph);

        env.execute();
    }
}
