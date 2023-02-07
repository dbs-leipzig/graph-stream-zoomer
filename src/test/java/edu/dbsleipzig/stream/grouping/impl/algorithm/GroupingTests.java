package edu.dbsleipzig.stream.grouping.impl.algorithm;

import edu.dbsleipzig.stream.grouping.impl.functions.utils.WindowConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraph;
import edu.dbsleipzig.stream.grouping.model.graph.StreamGraphConfig;
import edu.dbsleipzig.stream.grouping.model.graph.StreamTriple;
import edu.dbsleipzig.stream.grouping.model.graph.StreamVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class GroupingTests {
    private static Timestamp t1,t2,t3,t4;
    private static Timestamp windowT1, windowT2, windowT3, windowT4;
    private static GraphStreamGrouping groupingNoKeysNoAgg;
    private static StreamGraph streamGraph;
    static final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    static final ArrayList<StreamTriple> expectedNoKeyNoAgg = new ArrayList<>();

    @BeforeClass
    public static void setup() {
        DataStream<StreamTriple> tripleDataStream = createStreamTriples(env);
        streamGraph = StreamGraph.fromFlinkStream(tripleDataStream, new StreamGraphConfig(env));
        groupingNoKeysNoAgg = new TableGroupingBase.GroupingBuilder()
                .setWindowSize(10, WindowConfig.TimeUnit.SECONDS)
                .build();
    }

    @Test
    public void testGrouping() throws Exception {
        streamGraph = groupingNoKeysNoAgg.execute(streamGraph);
        windowT1 = getWindowToTimestamp(t1);
        windowT2 = getWindowToTimestamp(t2);
        windowT3 = getWindowToTimestamp(t3);
        windowT4 = getWindowToTimestamp(t4);
        ArrayList<StreamTriple> groupedTripleList = (ArrayList<StreamTriple>) streamGraph.toTripleStream().executeAndCollect(5);
        for (int i=0; i<groupedTripleList.size(); i++) {
            StreamTriple triple = groupedTripleList.get(i);
            assertTrue(triple.f2.equals(""));
            assertTrue(triple.f3.isEmpty());
            assertTrue(triple.f4.getVertexProperties().isEmpty());
            assertTrue(triple.f4.getVertexLabel().equals(""));
            assertTrue(triple.f5.getVertexProperties().isEmpty());
            assertTrue(triple.f5.getVertexLabel().equals(""));
            switch (i) {
                case 1: assertTrue(triple.getTimestamp().equals(windowT1));
                case 2: assertTrue(triple.getTimestamp().equals(windowT2));
                case 3: assertTrue(triple.getTimestamp().equals(windowT3));
                case 4: assertTrue(triple.getTimestamp().equals(windowT4));
            }
        }
    }

    private static Timestamp getWindowToTimestamp(Timestamp t) {
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
        t.setSeconds((t.getSeconds()/10+1)*10);
        Timestamp window = new Timestamp(t.getTime()-1);
        return window;

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
        t1 = new Timestamp(1619511660000L);
        t2 = new Timestamp(1619511670000L);
        t3 = new Timestamp(1619511680000L);
        t4 = new Timestamp(1619511690000L);

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

        return env.fromElements(edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8, edge9);
    }
}
