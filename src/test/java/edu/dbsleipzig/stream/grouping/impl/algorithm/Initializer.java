package edu.dbsleipzig.stream.grouping.impl.algorithm;

import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.gradoop.common.model.impl.properties.Properties;

import java.sql.Timestamp;
import java.util.HashMap;

public class Initializer {
    public static DataStream<StreamTriple> createStreamTriples(StreamExecutionEnvironment env) {

        /*
        t(i+1) = t(i) + 10s
         */
        Timestamp t1 = new Timestamp(1619511660000L);
        Timestamp t2 = new Timestamp(1619511670000L);
        Timestamp t3 = new Timestamp(1619511680000L);
        Timestamp t4 = new Timestamp(1619511690000L);

        /*
        Create vertices with empty properties
         */
        StreamVertex v1 = new StreamVertex("v1", "A", Properties.create(), t1);
        StreamVertex v2 = new StreamVertex("v2", "B", Properties.create(), t1);
        StreamVertex v3 = new StreamVertex("v3", "A", Properties.create(), t1);
        StreamVertex v4 = new StreamVertex("v4", "B", Properties.create(), t1);
        StreamVertex v5 = new StreamVertex("v5", "A", Properties.create(), t2);
        StreamVertex v6 = new StreamVertex("v6", "B", Properties.create(), t2);
        StreamVertex v7 = new StreamVertex("v7", "A", Properties.create(), t2);
        StreamVertex v8 = new StreamVertex("v8", "B", Properties.create(), t2);

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
        StreamTriple edge3 = new StreamTriple("e3", t1, "calculates", propertiesE3, v3, v4);
        StreamTriple edge4 = new StreamTriple("e4", t1, "impacts",  propertiesE1, v1, v2);
        StreamTriple edge5 = new StreamTriple("e5", t2, "impacts", propertiesE2, v5, v6);
        StreamTriple edge6 = new StreamTriple("e6", t2, "calculates", propertiesE3, v5, v6);
        StreamTriple edge7 = new StreamTriple("e7", t2, "impacts",  propertiesE1, v7, v8);
        StreamTriple edge8 = new StreamTriple("e8", t2, "impacts", propertiesE2, v7, v8);

        return env.fromElements(edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8);
    }

    public static Timestamp getWindowToTimestamp(Timestamp t) {
        /*
          TsTot1 = new Timestamp(t1.getTime());
        TsTot2 = new Timestamp(t2.getTime());
        TsTot3 = new Timestamp(t3.getTime());
        TsTot4 = new Timestamp(t4.getTime());

        TsTot1.setSeconds((TsTot1.getSeconds()/10+1) * 10);
        TsTot2.setSeconds((TsTot2.getSeconds()/10+1) * 10);
        TsTot3.setSeconds((TsTot3.getSeconds()/10+1) * 10);
        TsTot4.setSeconds((TsTot4.getSeconds()/10+1) * 10);

        windowOfT1 = new Timestamp(TsTot1.getTime() - 1);
         */
        Timestamp tmp = t;
        tmp.setSeconds((tmp.getSeconds()/10+1)*10);
        Timestamp window = new Timestamp(tmp.getTime()-1);
        return window;
    }

    public static StreamExecutionEnvironment getExecutionEnvironment() {
        Configuration cfg = new Configuration();
        int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();
        cfg.setString("taskmanager.memory.network.max", "1gb");
        return StreamExecutionEnvironment.createLocalEnvironment(defaultLocalParallelism, cfg);
    }

    public static StreamTableEnvironment getStreamTableEnvironment(StreamExecutionEnvironment env) {
        return StreamTableEnvironment.create(env);
    }

    public static StreamGraph getStreamGraph() {
        StreamExecutionEnvironment env = getExecutionEnvironment();
        return StreamGraph.fromFlinkStream(createStreamTriples(env), new StreamGraphConfig(env));
    }
}
