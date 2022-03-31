package edu.leipzig.impl.algorithm;

import edu.leipzig.impl.functions.aggregation.AvgProperty;
import edu.leipzig.impl.functions.aggregation.MinProperty;
import edu.leipzig.impl.functions.aggregation.SumProperty;
import edu.leipzig.impl.functions.utils.CreateSuperElementId;
import edu.leipzig.model.graph.StreamEdge;
import edu.leipzig.model.graph.StreamGraph;
import edu.leipzig.model.graph.StreamGraphConfig;
import edu.leipzig.model.graph.StreamTriple;
import edu.leipzig.model.graph.StreamVertex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.leipzig.model.table.TableSet.*;
import static org.apache.flink.table.api.Expressions.*;
import static org.junit.Assert.assertTrue;

/**
 * TODO: These unit/integration tests are messy
 * Edges with same source and target vertex but different labels lead to double vertex entry in vertices
 * table
 */
public class GraphStreamGroupingTest {

  private StreamVertex v1, v2, v3, v4, v5, v6, v7, v8;
  private StreamTriple edge1, edge2, edge3, edge4, edge5, edge6, edge7, edge8, edge9;
  private Timestamp t1, t2, t3, t4;
  private HashMap<String, Object> propertiesVertexV1, propertiesVertexV2, propertiesVertexV3,
    propertiesVertexV4;
  HashMap<String, Object> propertiesEdge1,propertiesEdge2, propertiesEdge3;
  private Properties propertiesV1, propertiesV2, propertiesV3, propertiesV4;
  private Properties propertiesE1, propertiesE2, propertiesE3;
  private DataStream<StreamTriple> testStream;
  private StreamGraph streamGraph;
  TableEnvironment tEnv;
  Table vertices, edges;
  TableGroupingBase.GroupingBuilder groupingBuilder;
  GraphStreamGrouping graphStreamGrouping;
  StreamTableEnvironment streamTableEnvironment;
  Table preparedVertices, furtherPreparedVertices, groupedVertices, newVertices;
  Timestamp TsTot1, TsTot2, TsTot3, TsTot4,  windowOfT1, windowOfT2, windowOfT3, windowOfT4;
  final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
    new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(2)
        .setNumberTaskManagers(1)
        .build());

  @Before
  public void init(){
    EnvironmentSettings settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build();
    tEnv = TableEnvironment.create(settings);

    t1 = new Timestamp(1619511661000L);
    t2 = new Timestamp(1619511662000L);
    t3 = new Timestamp(1619511673000L);
    t4 = new Timestamp(1619511674000L);

    v1 = new StreamVertex("v1", "A", Properties.create(), t1);
    v2 = new StreamVertex("v2", "B", Properties.create(), t2);
    v3 = new StreamVertex("v3", "A", Properties.create(), t3);
    v4 = new StreamVertex("v4", "B", Properties.create(), t4);
    v5 = new StreamVertex("v5", "A", Properties.create(), t1);
    v6 = new StreamVertex("v6", "B", Properties.create(), t2);
    v7 = new StreamVertex("v7", "A", Properties.create(), t3);
    v8 = new StreamVertex("v8", "B", Properties.create(), t4);

    propertiesVertexV1 = new HashMap<>();
    propertiesVertexV1.put("Relevance", 1);
    propertiesVertexV1.put("Size", 15);
    propertiesVertexV1.put("Weekday", "Monday");
    propertiesV1 = Properties.createFromMap(propertiesVertexV1);

    propertiesVertexV2 = new HashMap<>();
    propertiesVertexV2.put("Relevance", 3);
    propertiesVertexV2.put("Size", 15);
    propertiesVertexV2.put("Weekday", "Tuesday");
    propertiesV2 = Properties.createFromMap(propertiesVertexV2);


    propertiesVertexV3 = new HashMap<>();
    propertiesVertexV3.put("Relevance", 2);
    propertiesVertexV3.put("Size", 30);
    propertiesVertexV3.put("Weekday", "Monday");
    propertiesV3 = Properties.createFromMap(propertiesVertexV3);

    propertiesVertexV4 = new HashMap<>();
    propertiesVertexV4.put("Relevance", 5);
    propertiesVertexV4.put("Size", 5);
    propertiesVertexV4.put("Weekday", "Thursday");
    propertiesV4 = Properties.createFromMap(propertiesVertexV4);

    v1.setVertexProperties(propertiesV1);
    v2.setVertexProperties(propertiesV2);
    v3.setVertexProperties(propertiesV3);
    v4.setVertexProperties(propertiesV4);
    v5.setVertexProperties(propertiesV1);
    v6.setVertexProperties(propertiesV2);
    v7.setVertexProperties(propertiesV3);
    v8.setVertexProperties(propertiesV4);

    propertiesEdge1 = new HashMap<>();
    propertiesEdge1.put("Weight", 5);
    propertiesEdge1.put("Weekday", "Thursday");
    propertiesE1 = Properties.createFromMap(propertiesEdge1);

    propertiesEdge2 = new HashMap<>();
    propertiesEdge2.put("Weight", 6);
    propertiesEdge2.put("Weekday", "Wednesday");
    propertiesE2 = Properties.createFromMap(propertiesEdge2);

    propertiesEdge3 = new HashMap<>();
    propertiesEdge3.put("Weekday", "Thursday");
    propertiesEdge3.put("Weight", 3);
    propertiesE3 = Properties.createFromMap(propertiesEdge3);

    edge1 = new StreamTriple("e1", t1, "impacts",  propertiesE1, v1, v2);
    edge2 = new StreamTriple("e2", t1, "impacts", propertiesE2, v3, v4);
    edge3 = new StreamTriple("e3", t1, "calculates", propertiesE3, v3, v4);
    edge4 = new StreamTriple("e4", t1, "impacts",  propertiesE1, v1, v2);
    edge5 = new StreamTriple("e5", t1, "impacts", propertiesE2, v5, v6);
    edge6 = new StreamTriple("e6", t1, "calculates", propertiesE3, v5, v6);
    edge7 = new StreamTriple("e7", t1, "impacts",  propertiesE1, v7, v8);
    edge8 = new StreamTriple("e8", t1, "impacts", propertiesE2, v7, v8);
    edge9 = new StreamTriple("e9", t1, "calculates", propertiesE3, v7, v8);

    TsTot1 = new Timestamp(t1.getTime());
    TsTot2 = new Timestamp(t2.getTime());
    TsTot3 = new Timestamp(t3.getTime());
    TsTot4 = new Timestamp(t4.getTime());

    TsTot1.setSeconds((TsTot1.getSeconds()/10+1) * 10);
    TsTot2.setSeconds((TsTot2.getSeconds()/10+1) * 10);
    TsTot3.setSeconds((TsTot3.getSeconds()/10+1) * 10);
    TsTot4.setSeconds((TsTot4.getSeconds()/10+1) * 10);

    windowOfT1 = new Timestamp(TsTot1.getTime() - 1);
    windowOfT2 = new Timestamp(TsTot2.getTime() - 1);
    windowOfT3 = new Timestamp(TsTot3.getTime() - 1);
    windowOfT4 = new Timestamp(TsTot4.getTime() - 1);

    testStream = env.fromElements(edge1, edge2, edge3,  edge5, edge6, edge7, edge8, edge9);
    streamGraph = StreamGraph.fromFlinkStream(testStream, new StreamGraphConfig(env));

    vertices = streamGraph.getTableSet().getVertices();
    edges = streamGraph.getTableSet().getEdges();

    groupingBuilder = new TableGroupingBase.GroupingBuilder();
    groupingBuilder.addVertexGroupingKey(":label");
    groupingBuilder.addVertexGroupingKey("Weekday");
    groupingBuilder.addVertexAggregateFunction(new SumProperty("Relevance"));
    groupingBuilder.addVertexAggregateFunction(new AvgProperty("Size"));
    groupingBuilder.addEdgeGroupingKey(":label");
    groupingBuilder.addEdgeGroupingKey("Weekday");
    groupingBuilder.addEdgeAggregateFunction(new MinProperty("Weight"));
    graphStreamGrouping = groupingBuilder.build();

    graphStreamGrouping.setConfig(streamGraph.getConfig());
    graphStreamGrouping.setTableSet(streamGraph.getTableSet());
    streamTableEnvironment = graphStreamGrouping.getTableEnv();

    preparedVertices = graphStreamGrouping.prepareVertices();
    furtherPreparedVertices =
      graphStreamGrouping.convertLegacyToRawType(
        graphStreamGrouping.prepareVerticesFurther(preparedVertices));
    groupedVertices =
      graphStreamGrouping.convertLegacyToRawType(
        graphStreamGrouping.groupVertices(furtherPreparedVertices));
    newVertices = graphStreamGrouping.convertLegacyToRawType(
      graphStreamGrouping.createNewVertices(groupedVertices));



  }

 class StreamToHashMapPreparedVertices implements  MapFunction<Row, HashMap<String, String>>  {
    public HashMap<String,String> map(Row row){
      HashMap<String, String> toReturn = new HashMap<>();
      Properties vertexPropertyContent = (Properties) row.getField("vertex_properties");
      String key = row.getField("vertex_id").toString();
      String value = "";
      value += "label: "+ row.getField("vertex_label");
      for (Property p : vertexPropertyContent) {
        value += ", " + p.getKey() + ": " +  p.getValue();
      }
      value += ", vertex_event_time: " +row.getField("vertex_event_time").toString().replaceAll("T", " ");
      toReturn.put(key, value);
      return toReturn;
    }
  }

  class StreamToHashMapFurtherPreparedVertices implements  MapFunction<Row, HashMap<String, String>> {
    public HashMap<String, String> map(Row row){
      //TMP_0 -> Weekday, TMP_1 -> Relevance, TMP_2 -> Size
      HashMap<String, String> toReturn = new HashMap<>();
      String key = row.getField("vertex_id").toString();
      String value = "label: " + row.getField("vertex_label");
      value += ", vertex_event_time: " + row.getField("vertex_event_time")
        .toString().replaceAll("T", " ");
      value += ", Weekday: " + row.getField("TMP_0");
      value += ", Relevance: " + row.getField("TMP_1");
      value += ", Size: " + row.getField("TMP_2");
      toReturn.put(key, value);
      return toReturn;
    }
  }

  class StreamToHashMapGroupedVertices implements  MapFunction<Row, HashMap<String, String>> {
    public HashMap<String, String> map(Row row){
      HashMap<String, String> toReturn = new HashMap<>();
      String key = row.getField("super_vertex_label").toString();
      key += ", " + row.getField("TMP_3");
      key += ", " + row.getField("super_vertex_rowtime").toString().replaceAll("T", " ");

      String value = "sum_Relevance: " + row.getField("TMP_4");
      value += ", avg_Size: " + row.getField("TMP_5");
      toReturn.put(key, value);
      return  toReturn;
    }
  }

  class StreamToHashMapNewVertices implements  MapFunction<Row, HashMap<String, String>>  {
    List<String> localVertexGroupingPropertyKeys;
    boolean localUseVertexLabel;
    StreamToHashMapNewVertices(boolean localUseVertexLabel, List<String> localVertexGroupingPropertyKeys){
      //Create local instance of variables of outer class so inner method is serializable
      this.localVertexGroupingPropertyKeys = localVertexGroupingPropertyKeys;
      this.localUseVertexLabel = localUseVertexLabel;
    }
    public HashMap<String,String> map(Row row){
      HashMap<String, String> toReturn = new HashMap<>();

      /*
      Create keys from values of vertex grouping properties
      E.g. label, weekday -> key: A, Monday, timestamp
       */
      String key = "";
      Properties vertexPropertyContent = (Properties) row.getField("vertex_properties");
      if (localUseVertexLabel) {
        key += row.getField("vertex_label").toString();
      }
      for (String s : localVertexGroupingPropertyKeys) {
        if (key.equals("")) {
          key = vertexPropertyContent.get("\""+s+"\"").toString();
        }
        else {
          key += ", " + vertexPropertyContent.get("\""+s+"\"").toString();
        }
      }
      key += ", " +  row.getField("event_time").toString().replaceAll("T", " ");

      /*
      Value of the generated key is every property of the vertex that is not contained in the key already
       */
      String value = "";
      if (!localUseVertexLabel){
        value = row.getField("vertex_properties").toString();
      }
      for (Property p : vertexPropertyContent) {
        if (!localVertexGroupingPropertyKeys.contains(p.getKey().replaceAll("\"", ""))) {
          if (value.equals("")) {
            value = p.getKey() + ":" + p.getValue();
          }
          else {
            value += "; " + p.getKey() + ":" + p.getValue();
          }
        }
      }
      toReturn.put(key, value);
      return toReturn;
    }
  }




  @Test
  public void testPrepareVerticesMethod() throws Exception{
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(preparedVertices);
    HashMap<String, String> hashMapVertices = new HashMap<>();
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMapPreparedVertices()).executeAndCollect(10);
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    String propertiesV1ToCompare = hashMapVertices.get(v1.getVertexId());
    String propertiesV2ToCompare = hashMapVertices.get(v2.getVertexId());
    String propertiesV3ToCompare = hashMapVertices.get(v3.getVertexId());
    String propertiesV4ToCompare = hashMapVertices.get(v4.getVertexId());
    String propertiesV5ToCompare = hashMapVertices.get(v5.getVertexId());
    String propertiesV6ToCompare = hashMapVertices.get(v6.getVertexId());
    String propertiesV7ToCompare = hashMapVertices.get(v7.getVertexId());
    String propertiesV8ToCompare = hashMapVertices.get(v8.getVertexId());


    assertTrue(propertiesV1ToCompare.contains("Relevance: 1") &&
      propertiesV1ToCompare.contains("Weekday: Monday") && propertiesV1ToCompare.contains("Size: 15")
      && propertiesV1ToCompare.contains("label: " + v1.getVertexLabel())
      && propertiesV1ToCompare.contains("vertex_event_time: "+windowOfT1));
    assertTrue(propertiesV2ToCompare.contains("Relevance: 3") &&
      propertiesV2ToCompare.contains("Weekday: Tuesday") && propertiesV2ToCompare.contains("Size: 15")
      && propertiesV2ToCompare.contains("label: " + v2.getVertexLabel())
      && propertiesV2ToCompare.contains("vertex_event_time: "+windowOfT2));;
    assertTrue(propertiesV3ToCompare.contains("Relevance: 2") &&
      propertiesV3ToCompare.contains("Weekday: Monday") && propertiesV3ToCompare.contains("Size: 30")
      && propertiesV3ToCompare.contains("label: " + v3.getVertexLabel())
      && propertiesV3ToCompare.contains("vertex_event_time: "+windowOfT3));
    assertTrue(propertiesV4ToCompare.contains("Relevance: 5") &&
      propertiesV4ToCompare.contains("Weekday: Thursday") && propertiesV4ToCompare.contains("Size: 5")
      && propertiesV4ToCompare.contains("label: " + v4.getVertexLabel())
      && propertiesV4ToCompare.contains("vertex_event_time: "+windowOfT4));
    assertTrue(propertiesV5ToCompare.contains("Relevance: 1") &&
      propertiesV5ToCompare.contains("Weekday: Monday") && propertiesV5ToCompare.contains("Size: 15")
      && propertiesV5ToCompare.contains("label: " + v5.getVertexLabel())
      && propertiesV5ToCompare.contains("vertex_event_time: "+windowOfT1));
    assertTrue(propertiesV6ToCompare.contains("Relevance: 3") &&
      propertiesV6ToCompare.contains("Weekday: Tuesday") && propertiesV6ToCompare.contains("Size: 15")
      && propertiesV6ToCompare.contains("label: " + v6.getVertexLabel())
      && propertiesV6ToCompare.contains("vertex_event_time: "+windowOfT2));
    assertTrue(propertiesV7ToCompare.contains("Relevance: 2") &&
      propertiesV7ToCompare.contains("Weekday: Monday") && propertiesV7ToCompare.contains("Size: 30")
      && propertiesV7ToCompare.contains("label: " + v7.getVertexLabel())
      && propertiesV7ToCompare.contains("vertex_event_time: "+windowOfT3));
    assertTrue(propertiesV8ToCompare.contains("Relevance: 5") &&
      propertiesV8ToCompare.contains("Weekday: Thursday") && propertiesV8ToCompare.contains("Size: 5")
      && propertiesV8ToCompare.contains("label: " + v8.getVertexLabel())
      && propertiesV8ToCompare.contains("vertex_event_time: "+windowOfT4));
  }

  @Test
  public void testFurtherPrepareVerticesMethod() throws Exception{
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(furtherPreparedVertices);
    HashMap<String, String> hashMapVertices = new HashMap<>();
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMapFurtherPreparedVertices()).executeAndCollect(10);
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    String propertiesV1ToCompare = hashMapVertices.get(v1.getVertexId());
    String propertiesV2ToCompare = hashMapVertices.get(v2.getVertexId());
    String propertiesV3ToCompare = hashMapVertices.get(v3.getVertexId());
    String propertiesV4ToCompare = hashMapVertices.get(v4.getVertexId());
    String propertiesV5ToCompare = hashMapVertices.get(v5.getVertexId());
    String propertiesV6ToCompare = hashMapVertices.get(v6.getVertexId());
    String propertiesV7ToCompare = hashMapVertices.get(v7.getVertexId());
    String propertiesV8ToCompare = hashMapVertices.get(v8.getVertexId());

    assertTrue(propertiesV1ToCompare.contains("Relevance: 1") &&
      propertiesV1ToCompare.contains("Weekday: Monday") && propertiesV1ToCompare.contains("Size: 15")
      && propertiesV1ToCompare.contains("label: " + v1.getVertexLabel())
      && propertiesV1ToCompare.contains("vertex_event_time: "+windowOfT1));
    assertTrue(propertiesV2ToCompare.contains("Relevance: 3") &&
      propertiesV2ToCompare.contains("Weekday: Tuesday") && propertiesV2ToCompare.contains("Size: 15")
      && propertiesV2ToCompare.contains("label: " + v2.getVertexLabel())
      && propertiesV2ToCompare.contains("vertex_event_time: "+windowOfT2));;
    assertTrue(propertiesV3ToCompare.contains("Relevance: 2") &&
      propertiesV3ToCompare.contains("Weekday: Monday") && propertiesV3ToCompare.contains("Size: 30")
      && propertiesV3ToCompare.contains("label: " + v3.getVertexLabel())
      && propertiesV3ToCompare.contains("vertex_event_time: "+windowOfT3));
    assertTrue(propertiesV4ToCompare.contains("Relevance: 5") &&
      propertiesV4ToCompare.contains("Weekday: Thursday") && propertiesV4ToCompare.contains("Size: 5")
      && propertiesV4ToCompare.contains("label: " + v4.getVertexLabel())
      && propertiesV4ToCompare.contains("vertex_event_time: "+windowOfT4));
    assertTrue(propertiesV5ToCompare.contains("Relevance: 1") &&
      propertiesV5ToCompare.contains("Weekday: Monday") && propertiesV5ToCompare.contains("Size: 15")
      && propertiesV5ToCompare.contains("label: " + v5.getVertexLabel())
      && propertiesV5ToCompare.contains("vertex_event_time: "+windowOfT1));
    assertTrue(propertiesV6ToCompare.contains("Relevance: 3") &&
      propertiesV6ToCompare.contains("Weekday: Tuesday") && propertiesV6ToCompare.contains("Size: 15")
      && propertiesV6ToCompare.contains("label: " + v6.getVertexLabel())
      && propertiesV6ToCompare.contains("vertex_event_time: "+windowOfT2));
    assertTrue(propertiesV7ToCompare.contains("Relevance: 2") &&
      propertiesV7ToCompare.contains("Weekday: Monday") && propertiesV7ToCompare.contains("Size: 30")
      && propertiesV7ToCompare.contains("label: " + v7.getVertexLabel())
      && propertiesV7ToCompare.contains("vertex_event_time: "+windowOfT3));
    assertTrue(propertiesV8ToCompare.contains("Relevance: 5") &&
      propertiesV8ToCompare.contains("Weekday: Thursday") && propertiesV8ToCompare.contains("Size: 5")
      && propertiesV8ToCompare.contains("label: " + v8.getVertexLabel())
      && propertiesV8ToCompare.contains("vertex_event_time: "+windowOfT4));
  }

  @Test
  public void testGroupVerticesMethod() throws Exception{
    groupedVertices.execute().print();
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(groupedVertices);
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMapGroupedVertices()).executeAndCollect(10);

    HashMap<String, String> hashMapVertices = new HashMap<>();
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    String propertiesV1ToCompare = hashMapVertices.get("A, Monday, " + windowOfT1);
    String propertiesV2ToCompare = hashMapVertices.get("B, Tuesday, "+ windowOfT2);
    String propertiesV3ToCompare = hashMapVertices.get("A, Monday, " + windowOfT3);
    String propertiesV4ToCompare = hashMapVertices.get("B, Thursday, " + windowOfT4);

    assertTrue(propertiesV1ToCompare.contains("sum_Relevance: 2") &&
      propertiesV1ToCompare.contains("avg_Size: 15"));
    assertTrue(propertiesV2ToCompare.contains("sum_Relevance: 6") &&
      propertiesV2ToCompare.contains("avg_Size: 15"));
    assertTrue(propertiesV3ToCompare.contains("sum_Relevance: 4") &&
      propertiesV3ToCompare.contains("avg_Size: 30"));
    assertTrue(propertiesV4ToCompare.contains("sum_Relevance: 10") &&
      propertiesV4ToCompare.contains("avg_Size: 5"));
  }

  @Test
  public void testCreateNewVerticesMethod() throws Exception{
    DataStream<Row> rowStream = streamTableEnvironment.toDataStream(newVertices);
    List<HashMap<String,String>> resultingMaps =
      rowStream.map(new StreamToHashMapNewVertices(graphStreamGrouping.useVertexLabels,
        graphStreamGrouping.vertexGroupingPropertyKeys)).executeAndCollect(10);

    HashMap<String, String> hashMapVertices = new HashMap<>();
    for (HashMap<String, String> result : resultingMaps) {
      for (String key : result.keySet()) {
        hashMapVertices.put(key, result.get(key));
      }
    }

    String propertiesV1ToCompare = hashMapVertices.get(v1.getVertexLabel() + ", Monday, " + windowOfT1);
    String propertiesV2ToCompare = hashMapVertices.get(v2.getVertexLabel() + ", Tuesday, "+ windowOfT2);
    String propertiesV3ToCompare = hashMapVertices.get(v3.getVertexLabel() + ", Monday, " + windowOfT3);
    String propertiesV4ToCompare = hashMapVertices.get(v4.getVertexLabel() + ", Thursday, " + windowOfT4);

    assertTrue(propertiesV1ToCompare.contains("\"sum_Relevance\":2") &&
      propertiesV1ToCompare.contains("\"avg_Size\":15"));
    assertTrue(propertiesV2ToCompare.contains("\"sum_Relevance\":6") &&
      propertiesV2ToCompare.contains("\"avg_Size\":15"));
    assertTrue(propertiesV3ToCompare.contains("\"sum_Relevance\":4") &&
      propertiesV3ToCompare.contains("\"avg_Size\":30"));
    assertTrue(propertiesV4ToCompare.contains("\"sum_Relevance\":10") &&
      propertiesV4ToCompare.contains("\"avg_Size\":5"));
  }
}